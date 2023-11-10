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

from autosubmit.job.job_common import Status
from log.log import AutosubmitError


def test_check_Alljobs_send_command1_raises_autosubmit_error(autosubmit_exp, mocker):
    exp = autosubmit_exp('a000')
    mocker.patch('autosubmit.platforms.paramiko_platform.sleep')
    mock_log = mocker.patch('autosubmit.platforms.paramiko_platform.Log')
    platform = exp.platform
    # Because it raises a NotImplementedError, but we want to skip it to test an error...
    platform.get_checkAlljobs_cmd = mocker.MagicMock()
    platform.get_checkAlljobs_cmd.side_effect = ['ls']
    # Raise the AE error here.
    platform.send_command = mocker.MagicMock()
    ae = AutosubmitError(message='Test', code=123, trace='ERR!')
    platform.send_command.side_effect = ae
    as_conf = mocker.MagicMock()
    as_conf.get_copy_remote_logs.return_value = None
    job = mocker.MagicMock()
    job.id = 'TEST'
    job.name = 'TEST'
    with raises(AutosubmitError) as e:
        # Retries is -1 so that it skips the retry code block completely,
        # as we are not interested in testing that part here.
        platform.check_Alljobs(
            job_list=[(job, None)],
            as_conf=as_conf,
            retries=-1)
    assert e.value.message == 'Some Jobs are in Unknown status'
    assert e.value.code == 6008
    assert e.value.trace is None

    assert mock_log.warning.called
    assert mock_log.warning.call_args[0][1] == job.id
    assert mock_log.warning.call_args[0][2] == platform.name
    assert mock_log.warning.call_args[0][3] == Status.UNKNOWN


def test_check_Alljobs_send_command2_raises_autosubmit_error(autosubmit_exp, mocker):
    exp = autosubmit_exp('a000')
    platform = exp.platform
    mocker.patch('autosubmit.platforms.paramiko_platform.sleep')
    # Because it raises a NotImplementedError, but we want to skip it to test an error...
    platform.get_checkAlljobs_cmd = mocker.MagicMock()
    platform.get_checkAlljobs_cmd.side_effect = ['ls']
    # Raise the AE error here.
    platform.send_command = mocker.MagicMock()
    ae = AutosubmitError(message='Test', code=123, trace='ERR!')
    # Here the first time ``send_command`` is called it returns None, but
    # the second time it will raise the AutosubmitError for our test case.
    platform.send_command.side_effect = [None, ae]
    # Also need to make this function return False...
    platform._check_jobid_in_queue = mocker.MagicMock(return_value=False)
    # Then it will query the job status of the job, see further down as we set it
    as_conf = mocker.MagicMock()
    as_conf.get_copy_remote_logs.return_value = None
    job = mocker.MagicMock()
    job.id = 'TEST'
    job.name = 'TEST'
    job.status = Status.UNKNOWN

    platform.get_queue_status = mocker.MagicMock(side_effect=None)

    with raises(AutosubmitError) as e:
        # Retries is -1 so that it skips the retry code block completely,
        # as we are not interested in testing that part here.
        platform.check_Alljobs(
            job_list=[(job, None)],
            as_conf=as_conf,
            retries=1)
    # AS raises an exception with the message using the previous exception's
    # ``error_message``, but error code 6000 and no trace.
    assert e.value.message == ae.error_message
    assert e.value.code == 6000
    assert e.value.trace is None
