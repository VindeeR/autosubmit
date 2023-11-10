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

from pytest import raises
from pytest_mock import MockerFixture
from typing import Callable

from log.log import AutosubmitCritical, AutosubmitError


def test_properties(autosubmit_exp: Callable):
    exp = autosubmit_exp('a000')
    platform = exp.platform
    props = {
        'name': 'foo',
        'host': 'localhost1',
        'user': 'sam',
        'project': 'proj1',
        'budget': 100,
        'reservation': 1,
        'exclusivity': True,
        'hyperthreading': True,
        'type': 'SuperSlurm',
        'scratch': '/scratch/1',
        'project_dir': '/proj1',
        'root_dir': '/root_1',
        'partition': 'inter',
        'queue': 'prio1'
    }
    for prop, value in props.items():
        setattr(platform, prop, value)
    for prop, value in props.items():
        assert value == getattr(platform, prop)


def test_slurm_platform_submit_script_raises_autosubmit_critical_with_trace(
        autosubmit_exp: Callable,
        mocker: MockerFixture):
    exp = autosubmit_exp('a000')

    platform = exp.platform

    package = mocker.MagicMock()
    package.jobs.return_value = []
    valid_packages_to_submit = [
        package
    ]

    ae = AutosubmitError(message='invalid partition', code=123, trace='ERR!')
    platform.submit_Script = mocker.MagicMock(side_effect=ae)

    # AS will handle the AutosubmitError above, but then raise an AutosubmitCritical.
    # This new error won't contain all the info from the upstream error.
    with raises(AutosubmitCritical) as e:
        platform.process_batch_ready_jobs(
            valid_packages_to_submit=valid_packages_to_submit,
            failed_packages=[]
        )

    # AS will handle the error and then later will raise another error message.
    # But the AutosubmitError object we created will have been correctly used
    # without raising any exceptions (such as AttributeError).
    assert e.value.message != ae.message
