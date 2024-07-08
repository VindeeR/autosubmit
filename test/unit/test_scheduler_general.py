import pytest
from pathlib import Path
from autosubmit.autosubmit import Autosubmit
import os
import pwd

from test.unit.utils.common import create_database, init_expid

def get_script_files_path():
    current_folder = Path(__file__).resolve().parent
    files_folder = os.path.join(current_folder, 'files')
    return files_folder
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
        init_expid(str(folder.join('autosubmitrc')), platform='local', create=False)
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

    @pytest.fixture(scope='class')
    def generate_cmds(self, prepare_scheduler):
        init_expid(os.environ["AUTOSUBMIT_CONFIGURATION"], platform='local', expid='t000', create=True)
        Autosubmit.inspect(expid='t000',check_wrapper=False,force=True, lst=None, filter_chunks=None, filter_status=None, filter_section=None)
        return prepare_scheduler

    def test_default_parameters(self, generate_cmds, scheduler_tmpdir):
        """
        Test that the default parameters are correctly set in the scheduler files. It is a comparasion line to line, so the new templates must match the same line order as the old ones. Additional default parameters must be filled in the files/base_{scheduler}.yml as well as any change in the order
        :param generate_cmds:
        :param scheduler_tmpdir:
        :return:
        """
        # Get all expected default parameters from files/base_{scheduler}.yml
        schedulers_to_test = ['pjm', 'slurm', 'ecaccess', 'ps']
        # Load the base file for each scheduler
        files_folder = get_script_files_path()
        default_data = {}
        for base_f in Path(files_folder).glob('base_*.cmd'):
            name = base_f.stem.split('_')[1].upper()
            default_data[name] = Path(base_f).read_text()
        for scheduler in schedulers_to_test:
            scheduler = scheduler.upper()
            # Get the expected default parameters for the scheduler
            expected = default_data[scheduler]
            # Get the actual default parameters for the scheduler
            actual = Path(f"{generate_cmds.strpath}/t000/tmp/t000_BASE_{scheduler}.cmd").read_text()
            # Remove all after # Autosubmit header
            # ###################
            # count number of lines in expected
            expected_lines = expected.split('\n')
            actual = actual.split('\n')[:len(expected_lines)]
            actual = '\n'.join(actual)
            # Compare line to line
            for i, (line1, line2) in enumerate(zip(expected.split('\n'), actual.split('\n'))):
                if "PJM -o" in line1 or "PJM -e" in line1 or "#SBATCH --output" in line1 or "#SBATCH --error" in line1: # output error will be different
                    continue
                elif "##" in line1 or "##" in line2: # comment line
                    continue
                elif "header" in line1 or "header" in line2: # header line
                    continue
                else:
                    assert line1 == line2



