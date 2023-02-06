from unittest import TestCase
from unittest.mock import MagicMock, patch
from autosubmit.generators.pyflow import generate, Running
from pyflow import Suite
from tempfile import TemporaryDirectory
from datetime import datetime
from autosubmit.job.job import Job, Status


class TestPyFlow(TestCase):

    def setUp(self) -> None:
        self.suite_name = 'a000'

    def _create_job(self, section, name, status, running, split=None):
        """Create an Autosubmit job with pre-defined values."""
        # TODO: maybe suggest a kwags approach in the constructor to expand to local vars?
        job = Job(name, 0, status, 0)
        job.section = section
        job.running = str(running)
        job.split = split
        job.file = 'templates/script.sh'
        return job

    def _get_jobs(self):
        """Create list of Autosubmit jobs. For these tests we use a very simple experiment workflow."""
        ini_job = self._create_job('INI', 'a000_INI', Status.COMPLETED, Running.ONCE)
        prep_job = self._create_job('PREP', 'a000_20000101_fc0_PREP', Status.READY, Running.MEMBER)
        prep_job.parents = {ini_job}
        sim_job_1 = self._create_job('SIM', 'a000_20000101_fc0_1_1_SIM', Status.QUEUING, Running.CHUNK, 1)
        sim_job_1.parent = {prep_job}
        sim_job_2 = self._create_job('SIM', 'a000_20000101_fc0_1_2_SIM', Status.QUEUING, Running.CHUNK, 2)
        sim_job_2.parent = {prep_job, sim_job_1}
        return [ini_job, prep_job, sim_job_1, sim_job_2]

    def _create_job_list(self, expid, dates=None, members=None, chunks=None, empty=False):
        if dates is None:
            dates = []
        if members is None:
            members = []
        if chunks is None:
            chunks = []
        job_list = MagicMock(expid=expid)
        job_list.get_date_list.return_value = dates
        job_list.get_member_list.return_value = members
        job_list.get_chunk_list.return_value = chunks
        job_list.get_all.return_value = [] if empty is True else self._get_jobs()
        return job_list

    def test_generate(self):
        with TemporaryDirectory() as temp_out_dir:
            tests = [
                {
                    'job_list': self._create_job_list('a000', [datetime(2000, 1, 1)], ['fc0'], ['1']),
                    'as_conf': None,
                    'options': ['-e', 'a000', '-o', temp_out_dir, '-s', 'localhost'],
                    'expected_error': None
                },
                {
                    'job_list': self._create_job_list('a000', [datetime(2000, 1, 1)], ['fc0'], ['1']),
                    'as_conf': None,
                    'options': ['-e', 'a000', '-o', temp_out_dir, '-s', 'localhost', '--quiet'],
                    'expected_error': None
                },
                {
                    'job_list': self._create_job_list('a001', [], [], [], empty=True),
                    'as_conf': None,
                    'options': ['-e', 'a001', '-o', temp_out_dir, '-s', 'localhost', '--quiet'],
                    'expected_error': None
                },
                {
                    'job_list': self._create_job_list('a002', [], [], [], empty=True),
                    'as_conf': None,
                    'options': ['-e', 'a002', '-o', None, '-s', 'localhost', '--quiet'],
                    'expected_error': TypeError
                }
            ]
            for test in tests:
                job_list = test['job_list']
                as_conf = test['as_conf']
                options = test['options']
                expected_error = test['expected_error']

                if expected_error is not None:
                    try:
                        generate(job_list, as_conf, options)
                        self.fail('Test case expected to fail')
                    except:
                        pass
                else:
                    generate(job_list, as_conf, options)
