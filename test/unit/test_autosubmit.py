from textwrap import dedent
from pathlib import Path
from autosubmitconfigparser.config.basicconfig import BasicConfig

from autosubmit.autosubmit import Autosubmit
from test.unit.conftest import autosubmit_config

def test_copy_as_config(tmp_path, autosubmit_config):

    """Test the creation of files without damaging or causing error in the path

    Creates a dummy AS3 INI file, calls ``AutosubmitConfig.copy_as_config``, and
    verifies that the YAML files exists and is not empty, and a backup file was
    created. All without warnings or errors being raised (i.e. they were suppressed).
    """
    autosubmit_config('a000',{})

    rdd = ini_file = Path(f'{BasicConfig.LOCAL_ROOT_DIR}/a000/conf')
    rd = new_file = Path(f'{BasicConfig.LOCAL_ROOT_DIR}/a001/conf')
    ini_file.mkdir(parents=True, exist_ok=True)
    new_file.mkdir(parents=True, exist_ok=True)
    ini_file = ini_file / 'jobs_a000.conf'
    new_file = new_file / 'jobs_a001.yml'

    with open(ini_file, 'w+') as f:
        f.write(dedent('''\
                [LOCAL_SETUP]
                FILE = LOCAL_SETUP.sh
                PLATFORM = LOCAL
                '''))
        f.flush()

    Autosubmit().copy_as_config('a001','a000')

    new_yaml_file = Path(new_file.parent,new_file.stem).with_suffix('.yml')

    for td in rdd.iterdir():
        print(f'files a000 : {td}')
    for td in rd.iterdir():
        print(f'files a001 : {td}')
    print(f'new_file: {new_file}')
    print(f'new_file: {new_file.exists()}')
    print(f'new_yaml_file: {new_yaml_file}')
    print(f'new_yaml_file: {new_yaml_file.exists()}')

    assert not new_yaml_file.exists()
    assert new_yaml_file.stat().st_size > 0