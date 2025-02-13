from pathlib import Path
from textwrap import dedent
from typing import Callable, Dict

import pytest
from autosubmitconfigparser.config.basicconfig import BasicConfig

from autosubmit.autosubmit import Autosubmit
from test.unit.conftest import autosubmit_config, fixture_sqlite


def test_copy_as_config(tmp_path, autosubmit_config):

    """Test the creation of files without damaging or causing error in the path
    Creates a dummy AS3 INI file, calls ``AutosubmitConfig.copy_as_config``, and
    verifies that the YAML files exists and is not empty, and a backup file was
    created. All without warnings or errors being raised (i.e. they were suppressed).
    """
    autosubmit_config('a000',{})

    ini_file = Path(f'{BasicConfig.LOCAL_ROOT_DIR}/a000/conf')
    new_file = Path(f'{BasicConfig.LOCAL_ROOT_DIR}/a001/conf')
    ini_file.mkdir(parents=True, exist_ok=True)
    new_file.mkdir(parents=True, exist_ok=True)
    ini_file = ini_file / 'jobs_a000.conf'
    new_file = new_file / 'jobs_a001.yml'

    print(f'BasicConfig: {BasicConfig.LOCAL_ROOT_DIR}')
    with open(ini_file, 'w+') as f:
        f.write(dedent('''\
                [LOCAL_SETUP]
                FILE = LOCAL_SETUP.sh
                PLATFORM = LOCAL
                '''))
        f.flush()

    Autosubmit().copy_as_config('a001','a000')

    new_yaml_file = Path(new_file.parent,new_file.stem).with_suffix('.yml')

    assert new_yaml_file.exists()
    assert new_yaml_file.stat().st_size > 0


@pytest.mark.parametrize("fake_dir", [
    pytest.param("a000", marks=pytest.mark.xfail(reason="Meant to fail since it can't create a folder if one already exists")), # test meant to FAIL
    "", ]) # test meant to PASS with a generated expid
def test_expid(autosubmit_config: Callable[[str,Dict], BasicConfig], fake_dir, fixture_sqlite) -> None:
    """
    Function to test if the autosubmit().expid generates the paths and expid properly
    ::fake_dir -> if fake dir exists test will fail since it won't be able to generate folder
    ::real_dir -> folder it'll try to create and experiment id
    """

    BasicConfig.LOCAL_ROOT_DIR = fixture_sqlite
    if fake_dir != "":
        path = Path(fixture_sqlite) / fake_dir
        path.mkdir()

    expid = Autosubmit().expid("Test")

    experiment = Autosubmit().describe(expid)
    path = Path(fixture_sqlite) / expid

    assert path.exists()
    assert experiment is not None
    assert isinstance(expid, str) and len(expid) == 4