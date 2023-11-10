# Copyright 2015-2023 Earth Sciences Department, BSC-CNS
# This file is part of Autosubmit.
#
# Autosubmit is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Autosubmit is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Autosubmit.  If not, see <http://www.gnu.org/licenses/>.

import tempfile
from itertools import permutations, product
from mock import Mock
from pathlib import Path
from textwrap import dedent

from autosubmit.autosubmit import Autosubmit
from autosubmit.experiment.experiment_common import new_experiment
from autosubmitconfigparser.config.basicconfig import BasicConfig

DESCRIPTION = "for testing"
VERSION = "test-version"


def test_create_new_experiment(mocker):
    db_common_mock = mocker.patch('autosubmit.experiment.experiment_common.db_common')
    current_experiment_id = "empty"
    _build_db_mock(current_experiment_id, db_common_mock)
    experiment_id = new_experiment(DESCRIPTION, VERSION)
    assert "a000" == experiment_id


def test_create_new_test_experiment(mocker):
    db_common_mock = mocker.patch('autosubmit.experiment.experiment_common.db_common')
    current_experiment_id = "empty"
    _build_db_mock(current_experiment_id, db_common_mock)
    experiment_id = new_experiment(DESCRIPTION, VERSION, True)
    assert "t000" == experiment_id


def test_create_new_operational_experiment(mocker):
    db_common_mock = mocker.patch('autosubmit.experiment.experiment_common.db_common')
    current_experiment_id = "empty"
    _build_db_mock(current_experiment_id, db_common_mock)
    experiment_id = new_experiment(DESCRIPTION, VERSION, False, True)
    assert "o000" == experiment_id


def test_create_new_experiment_with_previous_one(mocker):
    db_common_mock = mocker.patch('autosubmit.experiment.experiment_common.db_common')
    current_experiment_id = "a007"
    _build_db_mock(current_experiment_id, db_common_mock)
    experiment_id = new_experiment(DESCRIPTION, VERSION)
    assert "a007" == experiment_id


def test_create_new_test_experiment_with_previous_one(mocker):
    db_common_mock = mocker.patch('autosubmit.experiment.experiment_common.db_common')
    current_experiment_id = "t0ac"
    _build_db_mock(current_experiment_id, db_common_mock)
    experiment_id = new_experiment(DESCRIPTION, VERSION, True)
    assert "t0ac" == experiment_id


def test_create_new_operational_experiment_with_previous_one(mocker):
    db_common_mock = mocker.patch('autosubmit.experiment.experiment_common.db_common')
    current_experiment_id = "o113"
    _build_db_mock(current_experiment_id, db_common_mock)
    experiment_id = new_experiment(DESCRIPTION, VERSION, False, True)
    assert "o113" == experiment_id


def _build_db_mock(current_experiment_id, mock_db_common):
    mock_db_common.last_name_used = Mock(return_value=current_experiment_id)
    mock_db_common.check_experiment_exists = Mock(return_value=False)


def test_autosubmit_generate_config(mocker):
    resource_filename_mock = mocker.patch('autosubmit.autosubmit.resource_listdir')
    resource_listdir_mock = mocker.patch('autosubmit.autosubmit.resource_filename')
    expid = 'ff99'
    original_local_root_dir = BasicConfig.LOCAL_ROOT_DIR

    with tempfile.NamedTemporaryFile(suffix='.yaml',
                                     mode='w') as source_yaml, tempfile.TemporaryDirectory() as temp_dir:
        # Our processed and commented YAML output file must be written here
        Path(temp_dir, expid, 'conf').mkdir(parents=True)
        BasicConfig.LOCAL_ROOT_DIR = temp_dir

        source_yaml.write(
            dedent('''JOB:
JOBNAME: SIM
PLATFORM: local
CONFIG:
TEST: The answer?
ROOT: No'''))
        source_yaml.flush()
        resource_listdir_mock.return_value = [Path(source_yaml.name).name]
        resource_filename_mock.return_value = source_yaml.name

        parameters = {
            'JOB': {
                'JOBNAME': 'sim'
            },
            'CONFIG': {
                'CONFIG.TEST': '42'
            }
        }
        Autosubmit.generate_as_config(exp_id=expid, parameters=parameters)

        source_text = Path(source_yaml.name).read_text()
        source_name = Path(source_yaml.name)
        output_text = Path(temp_dir, expid, 'conf', f'{source_name.stem}_{expid}.yml').read_text()

        assert source_text != output_text
        assert '# sim' not in source_text
        assert '# sim' in output_text
        assert '# 42' not in source_text
        assert '# 42' in output_text

    # Reset the local root dir.
    BasicConfig.LOCAL_ROOT_DIR = original_local_root_dir


def test_autosubmit_generate_config_resource_listdir_order(mocker) -> None:
    """
    In https://earth.bsc.es/gitlab/es/autosubmit/-/issues/1063 we had a bug
    where we relied on the order of returned entries of ``pkg_resources.resource_listdir``
    (which is actually undefined per https://importlib-resources.readthedocs.io/en/latest/migration.html).

    We use the arrays below to test that defining a git minimal, we process only
    the expected files. We permute and then product the arrays to get as many test
    cases as possible.

    For every test case, we know that for dummy and minimal we get just one configuration
    template file used. But for other cases we get as many files as we have that are not
    ``*minimal.yml`` nor ``*dummy.yml``. In our test cases here, when not dummy and not minimal
    we must get 2 files, since we have ``include_me_please.yml`` and ``me_too.yml``.

    :return: None
    """

    resource_filename_mock = mocker.patch('autosubmit.autosubmit.resource_filename')
    resource_listdir_mock = mocker.patch('autosubmit.autosubmit.resource_listdir')
    yaml_mock = mocker.patch('autosubmit.autosubmit.YAML.dump')

    # unique lists of resources, no repetitions
    resources = permutations(
        ['dummy.yml', 'local-minimal.yml', 'git-minimal.yml', 'include_me_please.yml', 'me_too.yml'])
    dummy = [True, False]
    local = [True, False]
    minimal_configuration = [True, False]
    test_cases = product(resources, dummy, local, minimal_configuration)
    keys = ['resources', 'dummy', 'local', 'minimal_configuration']

    for test_case in test_cases:
        test = dict(zip(keys, test_case))
        expid = 'ff99'
        original_local_root_dir = BasicConfig.LOCAL_ROOT_DIR

        with tempfile.TemporaryDirectory() as temp_dir:
            Path(temp_dir, expid, 'conf').mkdir(parents=True)
            BasicConfig.LOCAL_ROOT_DIR = temp_dir

            resources_return = []
            filenames_return = []

            for file_name in test['resources']:
                input_path = Path(temp_dir, file_name)
                with open(input_path, 'w+') as source_yaml:
                    source_yaml.write('TEST: YES')
                    source_yaml.flush()

                    resources_return.append(input_path.name)  # path
                    filenames_return.append(source_yaml.name)  # textiowrapper

            resource_listdir_mock.return_value = resources_return
            resource_filename_mock.side_effect = filenames_return

            Autosubmit.generate_as_config(
                exp_id=expid,
                dummy=test['dummy'],
                minimal_configuration=test['minimal_configuration'],
                local=test['local'])

            msg = f'Incorrect call count for resources={",".join(resources_return)}, dummy={test["dummy"]}, minimal_configuration={test["minimal_configuration"]}, local={test["local"]}'
            expected = 2 if (not test['dummy'] and not test['minimal_configuration']) else 1
            assert yaml_mock.call_count == expected, msg
            yaml_mock.reset_mock()

        # Reset the local root dir.
        BasicConfig.LOCAL_ROOT_DIR = original_local_root_dir
