from unittest import TestCase

from mock import Mock, patch, MagicMock

from autosubmit.job.job import Job
from autosubmit.job.job_common import Status
from autosubmit.job.job_packages import JobPackageSimple


class TestJobPackage(TestCase):

    def setUp(self):
        self.platform = Mock()
        self.jobs = [Job('dummy1', 0, Status.READY, 0),
                     Job('dummy2', 0, Status.READY, 0)]
        self.jobs[0]._platform = self.jobs[1]._platform = self.platform
        self.job_package = JobPackageSimple(self.jobs)

    def test_job_package_default_init(self):
        with self.assertRaises(Exception):
            JobPackageSimple([])

    def test_job_package_different_platforms_init(self):
        self.jobs[0]._platform = Mock()
        self.jobs[1]._platform = Mock()
        with self.assertRaises(Exception):
            JobPackageSimple(this.jobs)

    def test_job_package_none_platforms_init(self):
        self.jobs[0]._platform = None
        self.jobs[1]._platform = None
        with self.assertRaises(Exception):
            JobPackageSimple(this.jobs)

    def test_job_package_length(self):
        self.assertEquals(2, len(self.job_package))

    def test_job_package_jobs_getter(self):
        self.assertEquals(self.jobs, self.job_package.jobs)

    def test_job_package_platform_getter(self):
        self.assertEquals(self.platform, self.job_package.platform)

    @patch('os.path.exists')
    @patch('__builtin__.open')
    def test_job_package_submission(self, os_mock, open_mock):
        # arrange
        open_mock.return_value = MagicMock()
        os_mock.return_value = True
        for job in self.job_package.jobs:
            job._tmp_path = "fake-path"
            job.name = "fake-name"
            job._get_paramiko_template = Mock("false", "empty")
            job.file = "fake-file"
            job.update_parameters = MagicMock(return_value="fake-params")
            job.parameters = {"fake-params": "fake-value"}

        self.job_package._create_scripts = Mock()
        self.job_package._send_files = Mock()
        self.job_package._do_submission = Mock()
        configuration = Mock()
        configuration.get_project_type = Mock(return_value='fake-type')
        configuration.get_project_dir = Mock(return_value='fake-dir')
        configuration.get_project_name = Mock(return_value='fake-name')
        # act
        self.job_package.submit(configuration, 'fake-params')
        # assert
        for job in self.job_package.jobs:
            job.update_parameters.assert_called_once_with(configuration, 'fake-params')
        self.job_package._create_scripts.is_called_once_with()
        self.job_package._send_files.is_called_once_with()
        self.job_package._do_submission.is_called_once_with()
