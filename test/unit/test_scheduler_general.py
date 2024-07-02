import pytest
from pathlib import Path
from autosubmit.autosubmit import Autosubmit
from autosubmit.migrate.migrate import Migrate
from autosubmitconfigparser.config.configcommon import AutosubmitConfig
from autosubmitconfigparser.config.yamlparser import YAMLParserFactory
from autosubmitconfigparser.config.basicconfig import BasicConfig
import os

import pwd
from log.log import AutosubmitCritical

from test.unit.utils.common import create_database, generate_expid, create_expid

# Maybe this should be a regression test
class TestScheduler:

    @pytest.fixture(scope='class')
    def scheduler_tmpdir(self, tmpdir_factory):
        folder = tmpdir_factory.mktemp(f'scheduler_tests')
        os.mkdir(folder.join('scratch'))
        os.mkdir(folder.join('scheduler_tmp_dir'))
        file_stat = os.stat(f"{folder.strpath}")
        file_owner_id = file_stat.st_uid
        file_owner = pwd.getpwuid(file_owner_id).pw_name
        folder.owner = file_owner

        # Write an autosubmitrc file in the temporary directory
        autosubmitrc = folder.join('autosubmitrc')
        autosubmitrc.write(f'''
[database]
path = {folder}
filename = tests.db

[local]
path = {folder}

[globallogs]
path = {folder}

[structures]
path = {folder}

[historicdb]
path = {folder}

[historiclog]
path = {folder}

[defaultstats]
path = {folder}

''')
        os.environ['AUTOSUBMIT_CONFIGURATION'] = str(folder.join('autosubmitrc'))
        create_database(str(folder.join('autosubmitrc')))
        assert "tests.db" in [Path(f).name for f in folder.listdir()]
        generate_expid(str(folder.join('autosubmitrc')), platform='local')
        assert "t000" in [Path(f).name for f in folder.listdir()]
        return folder

    @pytest.fixture(scope='class')
    def prepare_scheduler(self, scheduler_tmpdir):
        # touch as_misc
        platforms_path = Path(f"{scheduler_tmpdir.strpath}/t000/conf/platforms_t000.yml")
        jobs_path = Path(f"{scheduler_tmpdir.strpath}/t000/conf/jobs_t000.yml")
        # Add each platform to test
        with platforms_path.open('w') as f:
            f.write(f"""
PLATFORMS:
    pytest-pjm:
        type: pjm
        host: 127.0.0.1
        user: {scheduler_tmpdir.owner}
        project: whatever
        scratch_dir: {scheduler_tmpdir}/scratch   
        MAX_WALLCLOCK: 48:00
        TEMP_DIR: ''
        MAX_PROCESSORS: 99999
        queue: dummy     
    pytest-slurm:
        type: slurm
        host: 127.0.0.1
        user: {scheduler_tmpdir.owner}
        project: whatever
        scratch_dir: {scheduler_tmpdir}/scratch       
        QUEUE: gp_debug
        ADD_PROJECT_TO_HOST: false
        MAX_WALLCLOCK: 48:00
        TEMP_DIR: ''
        MAX_PROCESSORS: 99999
    pytest-ecaccess:
        type: ecaccess
        version: slurm
        host: 127.0.0.1
        QUEUE: nf
        EC_QUEUE: hpc
        user: {scheduler_tmpdir.owner}
        project: whatever
        scratch_dir: {scheduler_tmpdir}/scratch       
    pytest-ps:
        type: ps
        host: 127.0.0.1
        user: {scheduler_tmpdir.owner}
        project: whatever
        scratch_dir: {scheduler_tmpdir}/scratch       
        """)
        # add a job of each platform type
        with jobs_path.open('w') as f:
            f.write(f"""
JOBS:
    job:
        SCRIPT: |
            echo "Hello World"
        For: 
            PLATFORM: [ pytest-pjm , pytest-slurm, pytest-ecaccess, pytest-ps]
            QUEUE: [dummy, gp_debug, nf, hpc]
            NAME: [pjm, slurm, ecaccess, ps]
        RUNNING: once
        wallclock: 00:01
        PROCESSORS: 5
        nodes: 2
        threads: 40
        tasks: 90
    base:
        SCRIPT: |
            echo "Hello World"
        For:
            PLATFORM: [ pytest-pjm , pytest-slurm, pytest-ecaccess, pytest-ps]
            QUEUE: [dummy, gp_debug, nf, hpc]
            NAME: [pjm, slurm, ecaccess, ps]
        RUNNING: once
        wallclock: 00:01
        """)



        expid_dir = Path(f"{scheduler_tmpdir.strpath}/scratch/whatever/{scheduler_tmpdir.owner}/t000")
        dummy_dir = Path(f"{scheduler_tmpdir.strpath}/scratch/whatever/{scheduler_tmpdir.owner}/t000/dummy_dir")
        real_data = Path(f"{scheduler_tmpdir.strpath}/scratch/whatever/{scheduler_tmpdir.owner}/t000/real_data")
        # write some dummy data inside scratch dir
        os.makedirs(expid_dir, exist_ok=True)
        os.makedirs(dummy_dir, exist_ok=True)
        os.makedirs(real_data, exist_ok=True)

        with open(dummy_dir.joinpath('dummy_file'), 'w') as f:
            f.write('dummy data')
        # create some dummy absolute symlinks in expid_dir to test migrate function
        os.symlink(dummy_dir.joinpath('dummy_file'), real_data.joinpath('dummy_symlink'))
        return scheduler_tmpdir

    @pytest.fixture
    def generate_cmds(self, prepare_scheduler):
        create_expid(os.environ["AUTOSUBMIT_CONFIGURATION"], 't000')
        Autosubmit.inspect(expid='t000',check_wrapper=False,force=True, lst=None, filter_chunks=None, filter_status=None, filter_section=None)
        return prepare_scheduler

    def test_check_slurm(self, generate_cmds, scheduler_tmpdir):
        # get t000_JOB_SLURM.cmd
        pjm_cmd = Path(f"{scheduler_tmpdir.strpath}/t000/tmp/t000_JOB_SLURM.cmd")
        pjm_basic_cmd = Path(f"{scheduler_tmpdir.strpath}/t000/tmp/t000_BASIC_SLURM.cmd")
        with pjm_cmd.open() as f:
            pjm_cmd_content = f.read()
            assert "pjsub" in pjm_cmd_content



