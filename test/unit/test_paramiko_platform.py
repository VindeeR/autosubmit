from collections import namedtuple
from unittest import TestCase

from shutil import rmtree
from tempfile import mkdtemp
from mock import MagicMock, patch

from autosubmit.job.job_common import Status
from autosubmit.platforms.paramiko_platform import ParamikoPlatform
from log.log import AutosubmitError


class TestParamikoPlatform(TestCase):

    Config = namedtuple('Config', ['LOCAL_ROOT_DIR', 'LOCAL_TMP_DIR'])

    def setUp(self):
        self.local_root_dir = mkdtemp()
        self.config = TestParamikoPlatform.Config(
            LOCAL_ROOT_DIR=self.local_root_dir,
            LOCAL_TMP_DIR='tmp'
        )
        self.platform = ParamikoPlatform(expid='a000', name='local', config=self.config)
        self.platform.job_status = {
            'COMPLETED': [],
            'RUNNING': [],
            'QUEUING': [],
            'FAILED': []
        }

    def tearDown(self):
        rmtree(self.local_root_dir)

    def test_paramiko_platform_constructor(self):
        assert self.platform.name == 'local'
        assert self.platform.expid == 'a000'
        assert self.config is self.platform.config

        # TODO: on the 3.15-branch branch the _header and _wrapper are
        #       not defined?
        # assert self.platform.header is None
        # assert self.platform.wrapper is None

        assert len(self.platform.job_status) == 4

    @patch('autosubmit.platforms.paramiko_platform.Log')
    @patch('autosubmit.platforms.paramiko_platform.sleep')
    def test_check_Alljobs_send_command1_raises_autosubmit_error(self, mock_sleep, mock_log):
        """
        Args:
            mock_sleep (MagicMock): mocking because the function sleeps for 5 seconds.
        """
        # Because it raises a NotImplementedError, but we want to skip it to test an error...
        self.platform.get_checkAlljobs_cmd = MagicMock()
        self.platform.get_checkAlljobs_cmd.side_effect = ['ls']
        # Raise the AE error here.
        self.platform.send_command = MagicMock()
        ae = AutosubmitError(message='Test', code=123, trace='ERR!')
        self.platform.send_command.side_effect = ae
        as_conf = MagicMock()
        as_conf.get_copy_remote_logs.return_value = None
        job = MagicMock()
        job.id = 'TEST'
        job.name = 'TEST'
        with self.assertRaises(AutosubmitError) as cm:
            # Retries is -1 so that it skips the retry code block completely,
            # as we are not interested in testing that part here.
            self.platform.check_Alljobs(
                job_list=[job],
                as_conf=as_conf,
                retries=-1)
        assert cm.exception.message == 'Remote pooling failed with error:ERR! Test\n Resetting platforms connections...'
        assert cm.exception.code == 6000
        # #assert cm.exception.trace is None
        #
        # assert mock_log.warning.called
        # assert mock_log.warning.call_args[0][1] == job.id
        # assert mock_log.warning.call_args[0][2] == self.platform.name
        # assert mock_log.warning.call_args[0][3] == Status.UNKNOWN

    @patch('autosubmit.platforms.paramiko_platform.sleep')
    def test_check_Alljobs_send_command2_raises_autosubmit_error(self, mock_sleep):
        """
        Args:
            mock_sleep (MagicMock): mocking because the function sleeps for 5 seconds.
        """
        # Because it raises a NotImplementedError, but we want to skip it to test an error...
        self.platform.get_checkAlljobs_cmd = MagicMock()
        self.platform.get_checkAlljobs_cmd.side_effect = ['ls']
        # Raise the AE error here.
        self.platform.send_command = MagicMock()
        ae = AutosubmitError(message='Test', code=123, trace='ERR!')
        # Here the first time ``send_command`` is called it returns None, but
        # the second time it will raise the AutosubmitError for our test case.
        self.platform.send_command.side_effect = [None, ae]
        # Also need to make this function return False...
        self.platform._check_jobid_in_queue = MagicMock(return_value = False)
        # Then it will query the job status of the job, see further down as we set it
        as_conf = MagicMock()
        as_conf.get_copy_remote_logs.return_value = None
        job = MagicMock()
        job.id = 'TEST'
        job.name = 'TEST'
        job.status = Status.UNKNOWN

        self.platform.get_queue_status = MagicMock(side_effect=None)

        with self.assertRaises(AutosubmitError) as cm:
            # Here the retries is 1
            self.platform.check_Alljobs(
                job_list=[job],
                as_conf=as_conf,
                retries=1)
        # AS raises an exception with the message using the previous exception's
        # ``error_message``, but error code 6000 and no trace.
        assert ae.error_message in cm.exception.message
        assert cm.exception.code == 6000
        assert cm.exception.trace is None
