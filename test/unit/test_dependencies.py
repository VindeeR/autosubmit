from unittest.mock import Mock

import copy
import inspect
import mock
import tempfile
import unittest
from copy import deepcopy
from datetime import datetime

from autosubmit.job.job_dict import DicJobs
from autosubmit.job.job import Job
from autosubmit.job.job_common import Status
from autosubmit.job.job_list import JobList
from autosubmit.job.job_list_persistence import JobListPersistenceDb
from autosubmitconfigparser.config.yamlparser import YAMLParserFactory


class FakeBasicConfig:
    def __init__(self):
        pass

    def props(self):
        pr = {}
        for name in dir(self):
            value = getattr(self, name)
            if not name.startswith('__') and not inspect.ismethod(value) and not inspect.isfunction(value):
                pr[name] = value
        return pr

    DB_DIR = '/dummy/db/dir'
    DB_FILE = '/dummy/db/file'
    DB_PATH = '/dummy/db/path'
    LOCAL_ROOT_DIR = '/dummy/local/root/dir'
    LOCAL_TMP_DIR = '/dummy/local/temp/dir'
    LOCAL_PROJ_DIR = '/dummy/local/proj/dir'
    DEFAULT_PLATFORMS_CONF = ''
    DEFAULT_JOBS_CONF = ''


class TestJobList(unittest.TestCase):
    def setUp(self):
        self.experiment_id = 'random-id'
        self.as_conf = mock.Mock()
        self.as_conf.experiment_data = dict()
        self.as_conf.experiment_data["JOBS"] = dict()
        self.as_conf.jobs_data = self.as_conf.experiment_data["JOBS"]
        self.as_conf.experiment_data["PLATFORMS"] = dict()
        self.temp_directory = tempfile.mkdtemp()
        self.JobList = JobList(self.experiment_id, FakeBasicConfig, YAMLParserFactory(),
                               JobListPersistenceDb(self.temp_directory, 'db'), self.as_conf)
        self.date_list = ["20020201", "20020202", "20020203", "20020204", "20020205", "20020206", "20020207",
                          "20020208", "20020209", "20020210"]
        self.member_list = ["fc1", "fc2", "fc3", "fc4", "fc5", "fc6", "fc7", "fc8", "fc9", "fc10"]
        self.chunk_list = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        self.split_list = [1, 2, 3, 4, 5]
        self.JobList._date_list = self.date_list
        self.JobList._member_list = self.member_list
        self.JobList._chunk_list = self.chunk_list
        self.JobList._split_list = self.split_list

        # Define common test case inputs here
        self.relationships_dates = {
            "DATES_FROM": {
                "20020201": {
                    "MEMBERS_FROM": {
                        "fc2": {
                            "DATES_TO": "[20020201:20020202]*,20020203",
                            "MEMBERS_TO": "fc2",
                            "CHUNKS_TO": "all"
                        }
                    },
                    "SPLITS_FROM": {
                        "ALL": {
                            "SPLITS_TO": "1"
                        }
                    }
                }
            }
        }
        self.relationships_dates_optional = deepcopy(self.relationships_dates)
        self.relationships_dates_optional["DATES_FROM"]["20020201"]["MEMBERS_FROM"] = {
            "fc2?": {"DATES_TO": "20020201", "MEMBERS_TO": "fc2", "CHUNKS_TO": "all", "SPLITS_TO": "5"}}
        self.relationships_dates_optional["DATES_FROM"]["20020201"]["SPLITS_FROM"] = {"ALL": {"SPLITS_TO": "1?"}}

        self.relationships_members = {
            "MEMBERS_FROM": {
                "fc2": {
                    "SPLITS_FROM": {
                        "ALL": {
                            "DATES_TO": "20020201",
                            "MEMBERS_TO": "fc2",
                            "CHUNKS_TO": "all",
                            "SPLITS_TO": "1"
                        }
                    }
                }
            }
        }
        self.relationships_chunks = {
            "CHUNKS_FROM": {
                "1": {
                    "DATES_TO": "20020201",
                    "MEMBERS_TO": "fc2",
                    "CHUNKS_TO": "all",
                    "SPLITS_TO": "1"
                }
            }
        }
        self.relationships_chunks2 = {
            "CHUNKS_FROM": {
                "1": {
                    "DATES_TO": "20020201",
                    "MEMBERS_TO": "fc2",
                    "CHUNKS_TO": "all",
                    "SPLITS_TO": "1"
                },
                "2": {
                    "SPLITS_FROM": {
                        "5": {
                            "SPLITS_TO": "2"
                        }
                    }
                }
            }
        }

        self.relationships_splits = {
            "SPLITS_FROM": {
                "1": {
                    "DATES_TO": "20020201",
                    "MEMBERS_TO": "fc2",
                    "CHUNKS_TO": "all",
                    "SPLITS_TO": "1"
                }
            }
        }

        self.relationships_general = {
            "DATES_TO": "20020201",
            "MEMBERS_TO": "fc2",
            "CHUNKS_TO": "all",
            "SPLITS_TO": "1"
        }
        self.relationships_general_1_to_1 = {
            "DATES_TO": "20020201",
            "MEMBERS_TO": "fc2",
            "CHUNKS_TO": "all",
            "SPLITS_TO": "1*,2*,3*,4*,5*"
        }
        # Create a mock Job object
        self.mock_job = Mock(wraps=Job)

        # Set the attributes on the mock object
        self.mock_job.name = "Job1"
        self.mock_job.job_id = 1
        self.mock_job.status = Status.READY
        self.mock_job.priority = 1
        self.mock_job.date = None
        self.mock_job.member = None
        self.mock_job.chunk = None
        self.mock_job.split = None

    def test_unify_to_filter(self):
        """Test the _unify_to_fitler function"""
        # :param unified_filter: Single dictionary with all filters_to
        # :param filter_to: Current dictionary that contains the filters_to
        # :param filter_type: "DATES_TO", "MEMBERS_TO", "CHUNKS_TO", "SPLITS_TO"
        # :return: unified_filter
        unified_filter = \
            {
                "DATES_TO": "20020201",
                "MEMBERS_TO": "fc2",
                "CHUNKS_TO": "all",
                "SPLITS_TO": "1"
            }
        filter_to = \
            {
                "DATES_TO": "20020205,[20020207:20020208]",
                "MEMBERS_TO": "fc2,fc3",
                "CHUNKS_TO": "all"
            }
        filter_type = "DATES_TO"
        result = self.JobList._unify_to_filter(unified_filter, filter_to, filter_type)
        expected_output = \
            {
                "DATES_TO": "20020201,20020205,20020207,20020208,",
                "MEMBERS_TO": "fc2",
                "CHUNKS_TO": "all",
                "SPLITS_TO": "1"
            }
        self.assertEqual(result, expected_output)

    def test_simple_dependency(self):
        result_d = self.JobList._check_dates({}, self.mock_job)
        result_m = self.JobList._check_members({}, self.mock_job)
        result_c = self.JobList._check_chunks({}, self.mock_job)
        result_s = self.JobList._check_splits({}, self.mock_job)
        self.assertEqual(result_d, {})
        self.assertEqual(result_m, {})
        self.assertEqual(result_c, {})
        self.assertEqual(result_s, {})

    def test_parse_filters_to_check(self):
        """Test the _parse_filters_to_check function"""
        result = self.JobList._parse_filters_to_check("20020201,20020202,20020203", self.date_list)
        expected_output = ["20020201", "20020202", "20020203"]
        self.assertEqual(result, expected_output)
        result = self.JobList._parse_filters_to_check("20020201,[20020203:20020205]", self.date_list)
        expected_output = ["20020201", "20020203", "20020204", "20020205"]
        self.assertEqual(result, expected_output)
        result = self.JobList._parse_filters_to_check("[20020201:20020203],[20020205:20020207]", self.date_list)
        expected_output = ["20020201", "20020202", "20020203", "20020205", "20020206", "20020207"]
        self.assertEqual(result, expected_output)
        result = self.JobList._parse_filters_to_check("20020201", self.date_list)
        expected_output = ["20020201"]
        self.assertEqual(result, expected_output)

    def test_parse_filter_to_check(self):
        # Call the function to get the result
        # Value can have the following formats:
        # a range: [0:], [:N], [0:N], [:-1], [0:N:M] ...
        # a value: N
        # a range with step: [0::M], [::2], [0::3], [::3] ...
        result = self.JobList._parse_filter_to_check("20020201", self.date_list)
        expected_output = ["20020201"]
        self.assertEqual(result, expected_output)
        result = self.JobList._parse_filter_to_check("[20020201:20020203]", self.date_list)
        expected_output = ["20020201", "20020202", "20020203"]
        self.assertEqual(result, expected_output)
        result = self.JobList._parse_filter_to_check("[20020201:20020203:2]", self.date_list)
        expected_output = ["20020201", "20020203"]
        self.assertEqual(result, expected_output)
        result = self.JobList._parse_filter_to_check("[20020202:]", self.date_list)
        expected_output = self.date_list[1:]
        self.assertEqual(result, expected_output)
        result = self.JobList._parse_filter_to_check("[:20020203]", self.date_list)
        expected_output = self.date_list[:3]
        self.assertEqual(result, expected_output)
        result = self.JobList._parse_filter_to_check("[::2]", self.date_list)
        expected_output = self.date_list[::2]
        self.assertEqual(result, expected_output)
        result = self.JobList._parse_filter_to_check("[20020203::]", self.date_list)
        expected_output = self.date_list[2:]
        self.assertEqual(result, expected_output)
        result = self.JobList._parse_filter_to_check("[:20020203:]", self.date_list)
        expected_output = self.date_list[:3]
        self.assertEqual(result, expected_output)
        # test with a member N:N
        result = self.JobList._parse_filter_to_check("[fc2:fc3]", self.member_list)
        expected_output = ["fc2", "fc3"]
        self.assertEqual(result, expected_output)
        # test with a chunk
        result = self.JobList._parse_filter_to_check("[1:2]", self.chunk_list, level_to_check="CHUNKS_FROM")
        expected_output = [1, 2]
        self.assertEqual(result, expected_output)
        # test with a split
        result = self.JobList._parse_filter_to_check("[1:2]", self.split_list, level_to_check="SPLITS_FROM")
        expected_output = [1, 2]
        self.assertEqual(result, expected_output)

    def test_check_dates(self):
        # Call the function to get the result
        self.mock_job.date = datetime.strptime("20020201", "%Y%m%d")
        self.mock_job.member = "fc2"
        self.mock_job.chunk = 1
        self.mock_job.split = 1
        result = self.JobList._check_dates(self.relationships_dates, self.mock_job)
        expected_output = {
            "DATES_TO": "20020201*,20020202*,20020203",
            "MEMBERS_TO": "fc2",
            "CHUNKS_TO": "all",
            "SPLITS_TO": "1"
        }
        self.assertEqual(result, expected_output)
        # failure
        self.mock_job.date = datetime.strptime("20020301", "%Y%m%d")
        result = self.JobList._check_dates(self.relationships_dates, self.mock_job)
        self.assertEqual(result, {})

    def test_check_members(self):
        # Call the function to get the result
        self.mock_job.date = datetime.strptime("20020201", "%Y%m%d")
        self.mock_job.member = "fc2"

        result = self.JobList._check_members(self.relationships_members, self.mock_job)
        expected_output = {
            "DATES_TO": "20020201",
            "MEMBERS_TO": "fc2",
            "CHUNKS_TO": "all",
            "SPLITS_TO": "1"
        }
        self.assertEqual(result, expected_output)
        self.mock_job.member = "fc3"
        result = self.JobList._check_members(self.relationships_members, self.mock_job)
        self.assertEqual(result, {})
        # FAILURE
        self.mock_job.member = "fc99"
        result = self.JobList._check_members(self.relationships_members, self.mock_job)
        self.assertEqual(result, {})

    def test_check_splits(self):
        # Call the function to get the result

        self.mock_job.split = 1
        result = self.JobList._check_splits(self.relationships_splits, self.mock_job)
        expected_output = {
            "DATES_TO": "20020201",
            "MEMBERS_TO": "fc2",
            "CHUNKS_TO": "all",
            "SPLITS_TO": "1"
        }
        self.assertEqual(result, expected_output)
        self.mock_job.split = 2
        result = self.JobList._check_splits(self.relationships_splits, self.mock_job)
        self.assertEqual(result, {})
        # failure
        self.mock_job.split = 99
        result = self.JobList._check_splits(self.relationships_splits, self.mock_job)
        self.assertEqual(result, {})

    def test_check_chunks(self):
        # Call the function to get the result

        self.mock_job.chunk = 1
        result = self.JobList._check_chunks(self.relationships_chunks, self.mock_job)
        expected_output = {
            "DATES_TO": "20020201",
            "MEMBERS_TO": "fc2",
            "CHUNKS_TO": "all",
            "SPLITS_TO": "1"
        }
        self.assertEqual(result, expected_output)
        self.mock_job.chunk = 2
        result = self.JobList._check_chunks(self.relationships_chunks, self.mock_job)
        self.assertEqual(result, {})
        # failure
        self.mock_job.chunk = 99
        result = self.JobList._check_chunks(self.relationships_chunks, self.mock_job)
        self.assertEqual(result, {})

    def test_check_general(self):
        # Call the function to get the result

        self.mock_job.date = datetime.strptime("20020201", "%Y%m%d")
        self.mock_job.member = "fc2"
        self.mock_job.chunk = 1
        self.mock_job.split = 1
        result = self.JobList._filter_current_job(self.mock_job, self.relationships_general)
        expected_output = {
            "DATES_TO": "20020201",
            "MEMBERS_TO": "fc2",
            "CHUNKS_TO": "all",
            "SPLITS_TO": "1"
        }
        self.assertEqual(result, expected_output)



    def test_check_relationship(self):
        relationships = {'MEMBERS_FROM': {
            'TestMember,   TestMember2,TestMember3   ': {'CHUNKS_TO': 'None', 'DATES_TO': 'None', 'FROM_STEP': None,
                                                         'MEMBERS_TO': 'None', 'STATUS': None}}}
        level_to_check = "MEMBERS_FROM"
        value_to_check = "TestMember"
        result = self.JobList._check_relationship(relationships, level_to_check, value_to_check)
        expected_output = [
            {'CHUNKS_TO': 'None', 'DATES_TO': 'None', 'FROM_STEP': None, 'MEMBERS_TO': 'None', 'STATUS': None}]
        self.assertEqual(result, expected_output)
        value_to_check = "TestMember2"
        result = self.JobList._check_relationship(relationships, level_to_check, value_to_check)
        expected_output = [
            {'CHUNKS_TO': 'None', 'DATES_TO': 'None', 'FROM_STEP': None, 'MEMBERS_TO': 'None', 'STATUS': None}]
        self.assertEqual(result, expected_output)
        value_to_check = "TestMember3"
        result = self.JobList._check_relationship(relationships, level_to_check, value_to_check)
        expected_output = [
            {'CHUNKS_TO': 'None', 'DATES_TO': 'None', 'FROM_STEP': None, 'MEMBERS_TO': 'None', 'STATUS': None}]
        self.assertEqual(result, expected_output)
        value_to_check = "TestMember   "
        result = self.JobList._check_relationship(relationships, level_to_check, value_to_check)
        expected_output = [
            {'CHUNKS_TO': 'None', 'DATES_TO': 'None', 'FROM_STEP': None, 'MEMBERS_TO': 'None', 'STATUS': None}]
        self.assertEqual(result, expected_output)
        value_to_check = "   TestMember"
        result = self.JobList._check_relationship(relationships, level_to_check, value_to_check)
        expected_output = [
            {'CHUNKS_TO': 'None', 'DATES_TO': 'None', 'FROM_STEP': None, 'MEMBERS_TO': 'None', 'STATUS': None}]
        self.assertEqual(result, expected_output)

    def apply_filter(self, possible_parents, filters_to, child_splits):
        nodes_added = []
        for parent in possible_parents:
            if parent.name == self.mock_job.name:
                continue
            splits_to = filters_to.get("SPLITS_TO", None)
            if splits_to:
                if not parent.splits:
                    parent_splits = 0
                else:
                    parent_splits = int(parent.splits)
                splits = max(child_splits, parent_splits)
                if splits > 0:
                    associative_list_splits = [str(split) for split in range(1, int(splits) + 1)]
                else:
                    associative_list_splits = None
                if JobList._apply_filter_1_to_1_splits(parent.split, splits_to, associative_list_splits, self.mock_job,
                                                       parent):
                    nodes_added.append(parent)
        return nodes_added

    # @mock.patch('autosubmit.job.job_dict.date2str')
    def test_get_jobs_filtered_and_apply_filter_1_to_1_splits(self):
        # This function is the new 1-to-1, 1-to-N and N-to-1 tests these previous tests should be here
        # To get possible_parents def get_jobs_filtered(self, section , job, filters_to, natural_date, natural_member ,natural_chunk )
        # To apply the filter def self._apply_filter_1_to_1_splits(parent.split, splits_to, associative_list_splits, job, parent):
        self.mock_job.date = datetime.strptime("20020204", "%Y%m%d")
        self.mock_job.chunk = 5
        once_jobs = [Job('Fake-Section-once', 1, Status.READY, 1), Job('Fake-Section-once2', 2, Status.READY, 1)]
        for job in once_jobs:
            job.date = None
            job.member = None
            job.chunk = None
            job.split = None
        date_jobs = [Job('Fake-section-date', 1, Status.READY, 1), Job('Fake-section-date2', 2, Status.READY, 1)]
        for job in date_jobs:
            job.date = datetime.strptime("20200128", "%Y%m%d")
            job.member = None
            job.chunk = None
            job.split = None
        member_jobs = [Job('Fake-section-member', 1, Status.READY, 1), Job('Fake-section-member2', 2, Status.READY, 1)]
        for job in member_jobs:
            job.date = datetime.strptime("20200128", "%Y%m%d")
            job.member = "fc0"
            job.chunk = None
            job.split = None
        chunk_jobs = [Job('Fake-section-chunk', 1, Status.READY, 1), Job('Fake-section-chunk2', 2, Status.READY, 1)]
        for index, job in enumerate(chunk_jobs):
            job.date = datetime.strptime("20200128", "%Y%m%d")
            job.member = "fc0"
            job.chunk = index + 1
            job.split = None
        split_jobs = [Job('Fake-section-split', 1, Status.READY, 1), Job('Fake-section-split2', 2, Status.READY, 1),
                      Job('Fake-section-split3', 3, Status.READY, 1), Job('Fake-section-split4', 4, Status.READY, 1)]
        for index, job in enumerate(split_jobs):
            job.date = datetime.strptime("20200128", "%Y%m%d")
            job.member = "fc0"
            job.chunk = 1
            job.split = index + 1
            job.splits = len(split_jobs)
        split_jobs2 = [Job('Fake-section-split', 1, Status.READY, 1), Job('Fake-section-split2', 2, Status.READY, 1),
                       Job('Fake-section-split3', 3, Status.READY, 1), Job('Fake-section-split4', 4, Status.READY, 1)]
        for index, job in enumerate(split_jobs2):
            job.date = datetime.strptime("20200128", "%Y%m%d")
            job.member = "fc0"
            job.chunk = 1
            job.split = index + 1
            job.splits = len(split_jobs2)
        jobs_dic = DicJobs(self.date_list, self.member_list, self.chunk_list, "hour", default_retrials=0,
                           as_conf=self.as_conf)
        date = "20200128"
        jobs_dic._dic = {
            'fake-section-once': once_jobs[0],
            'fake-section-date': {datetime.strptime(date, "%Y%m%d"): date_jobs[0]},
            'fake-section-member': {datetime.strptime(date, "%Y%m%d"): {"fc0": member_jobs[0]}},
            'fake-section-chunk': {datetime.strptime(date, "%Y%m%d"): {"fc0": {1: chunk_jobs[0], 2: chunk_jobs[1]}}},
            'fake-section-split': {datetime.strptime(date, "%Y%m%d"): {"fc0": {1: split_jobs}}},
            'fake-section-split2': {datetime.strptime(date, "%Y%m%d"): {"fc0": {1: split_jobs2[0:2]}}},
            'fake-section-dates': {datetime.strptime(date, "%Y%m%d"): date_jobs},
            'fake-section-members': {datetime.strptime(date, "%Y%m%d"): {"fc0": member_jobs}},
            'fake-section-chunks': {datetime.strptime(date, "%Y%m%d"): {"fc0": {1: chunk_jobs, 2: chunk_jobs}}},
            'fake-section-single-chunk': {datetime.strptime(date, "%Y%m%d"): {"fc0": {1: chunk_jobs[0]}}},
        }
        parent = copy.deepcopy(self.mock_job)
        # Get possible parents
        filters_to = {
            "DATES_TO": "20200128,20200129,20200130",
            "MEMBERS_TO": "fc0,fc1",
            "CHUNKS_TO": "1,2,3,4,5,6",
            "SPLITS_TO": "1*\\2,2*\\2,3*\\2,4*\\2"
        }
        self.mock_job.section = "fake-section-split"
        self.mock_job.running = "once"
        self.mock_job.split = 1
        self.mock_job.splits = 4
        self.mock_job.chunk = 1

        parent.section = "fake-section-split2"
        parent.splits = 2
        if not self.mock_job.splits:
            child_splits = 0
        else:
            child_splits = int(self.mock_job.splits)
        possible_parents = jobs_dic.get_jobs_filtered(parent.section, self.mock_job, filters_to, self.mock_job.date,
                                                      self.mock_job.member, self.mock_job.chunk)
        # Apply the filter
        nodes_added = self.apply_filter(possible_parents, filters_to, child_splits)
        # assert
        self.assertEqual(len(nodes_added), 2)
        filters_to = {
            "DATES_TO": "all",
            "MEMBERS_TO": "fc0,fc1",
            "CHUNKS_TO": "1,2,3,4,5,6",
            "SPLITS_TO": "1*\\2,2*\\2,3*\\2,4*\\2"
        }
        possible_parents = jobs_dic.get_jobs_filtered(parent.section, self.mock_job, filters_to, self.mock_job.date,
                                                      self.mock_job.member, self.mock_job.chunk)
        nodes_added = self.apply_filter(possible_parents, filters_to, child_splits)
        self.assertEqual(len(nodes_added), 2)
        filters_to = {
            "DATES_TO": "none",
            "MEMBERS_TO": "fc0,fc1",
            "CHUNKS_TO": "1,2,3,4,5,6",
            "SPLITS_TO": "1*\\2,2*\\2,3*\\2,4*\\2"
        }
        possible_parents = jobs_dic.get_jobs_filtered(parent.section, self.mock_job, filters_to, self.mock_job.date,
                                                      self.mock_job.member, self.mock_job.chunk)
        nodes_added = self.apply_filter(possible_parents, filters_to, child_splits)
        self.assertEqual(len(nodes_added), 0)
        filters_to = {
            "MEMBERS_TO": "fc0,fc1",
            "CHUNKS_TO": "1,2,3,4,5,6",
            "SPLITS_TO": "1*\\2,2*\\2,3*\\2,4*\\2"
        }
        possible_parents = jobs_dic.get_jobs_filtered(parent.section, self.mock_job, filters_to, self.mock_job.date,
                                                      self.mock_job.member, self.mock_job.chunk)
        nodes_added = self.apply_filter(possible_parents, filters_to, child_splits)
        self.assertEqual(len(nodes_added), 2)
        filters_to = {
            "MEMBERS_TO": "all",
            "CHUNKS_TO": "1,2,3,4,5,6",
            "SPLITS_TO": "1*\\2,2*\\2,3*\\2,4*\\2"
        }
        possible_parents = jobs_dic.get_jobs_filtered(parent.section, self.mock_job, filters_to, self.mock_job.date,
                                                      self.mock_job.member, self.mock_job.chunk)
        nodes_added = self.apply_filter(possible_parents, filters_to, child_splits)
        self.assertEqual(len(nodes_added), 2)
        filters_to = {
            "MEMBERS_TO": "none",
            "CHUNKS_TO": "1,2,3,4,5,6",
            "SPLITS_TO": "1*\\2,2*\\2,3*\\2,4*\\2"
        }
        possible_parents = jobs_dic.get_jobs_filtered(parent.section, self.mock_job, filters_to, self.mock_job.date,
                                                      self.mock_job.member, self.mock_job.chunk)
        nodes_added = self.apply_filter(possible_parents, filters_to, child_splits)
        self.assertEqual(len(nodes_added), 0)
        filters_to = {
            "CHUNKS_TO": "1,2,3,4,5,6",
            "SPLITS_TO": "1*\\2,2*\\2,3*\\2,4*\\2"
        }
        possible_parents = jobs_dic.get_jobs_filtered(parent.section, self.mock_job, filters_to, self.mock_job.date,
                                                      self.mock_job.member, self.mock_job.chunk)
        nodes_added = self.apply_filter(possible_parents, filters_to, child_splits)
        self.assertEqual(len(nodes_added), 2)
        filters_to = {
            "CHUNKS_TO": "all",
            "SPLITS_TO": "1*\\2,2*\\2,3*\\2,4*\\2"
        }
        possible_parents = jobs_dic.get_jobs_filtered(parent.section, self.mock_job, filters_to, self.mock_job.date,
                                                      self.mock_job.member, self.mock_job.chunk)
        nodes_added = self.apply_filter(possible_parents, filters_to, child_splits)
        self.assertEqual(len(nodes_added), 2)
        filters_to = {
            "CHUNKS_TO": "none",
            "SPLITS_TO": "1*\\2,2*\\2,3*\\2,4*\\2"
        }
        possible_parents = jobs_dic.get_jobs_filtered(parent.section, self.mock_job, filters_to, self.mock_job.date,
                                                      self.mock_job.member, self.mock_job.chunk)
        nodes_added = self.apply_filter(possible_parents, filters_to, child_splits)
        self.assertEqual(len(nodes_added), 0)
        filters_to = {
            "SPLITS_TO": "1*\\2,2*\\2,3*\\2,4*\\2"
        }
        possible_parents = jobs_dic.get_jobs_filtered(parent.section, self.mock_job, filters_to, self.mock_job.date,
                                                      self.mock_job.member, self.mock_job.chunk)
        nodes_added = self.apply_filter(possible_parents, filters_to, child_splits)
        self.assertEqual(len(nodes_added), 2)
        filters_to = {
            "SPLITS_TO": "all"
        }
        possible_parents = jobs_dic.get_jobs_filtered(parent.section, self.mock_job, filters_to, self.mock_job.date,
                                                      self.mock_job.member, self.mock_job.chunk)
        nodes_added = self.apply_filter(possible_parents, filters_to, child_splits)
        self.assertEqual(len(nodes_added), 2)
        filters_to = {
            "SPLITS_TO": "none"
        }
        possible_parents = jobs_dic.get_jobs_filtered(parent.section, self.mock_job, filters_to, self.mock_job.date,
                                                      self.mock_job.member, self.mock_job.chunk)
        nodes_added = self.apply_filter(possible_parents, filters_to, child_splits)
        self.assertEqual(len(nodes_added), 0)

        self.mock_job.date = datetime.strptime("20200128", "%Y%m%d")
        self.mock_job.member = None
        self.mock_job.chunk = None
        filters_to = {
            "DATES_TO": "all",
            "MEMBERS_TO": "all",
            "CHUNKS_TO": "all",
            "SPLITS_TO": "all"
        }
        parent.section = "fake-section-date"
        possible_parents = jobs_dic.get_jobs_filtered(parent.section, self.mock_job, filters_to, self.mock_job.date,
                                                      self.mock_job.member, self.mock_job.chunk)
        self.assertEqual(len(possible_parents), 1)
        parent.section = "fake-section-member"
        possible_parents = jobs_dic.get_jobs_filtered(parent.section, self.mock_job, filters_to, self.mock_job.date,
                                                      self.mock_job.member, self.mock_job.chunk)
        self.assertEqual(len(possible_parents), 1)
        parent.section = "fake-section-chunk"
        possible_parents = jobs_dic.get_jobs_filtered(parent.section, self.mock_job, filters_to, self.mock_job.date,
                                                      self.mock_job.member, 1)
        self.assertEqual(len(possible_parents), 2)
        parent.section = "fake-section-dates"
        possible_parents = jobs_dic.get_jobs_filtered(parent.section, self.mock_job, filters_to, self.mock_job.date,
                                                      self.mock_job.member, 1)
        self.assertEqual(len(possible_parents), 2)
        parent.section = "fake-section-members"
        possible_parents = jobs_dic.get_jobs_filtered(parent.section, self.mock_job, filters_to, self.mock_job.date,
                                                      "fc0", 1)
        self.assertEqual(len(possible_parents), 2)
        parent.section = "fake-section-chunks"
        possible_parents = jobs_dic.get_jobs_filtered(parent.section, self.mock_job, filters_to, self.mock_job.date,
                                                      "fc0", 1)
        self.assertEqual(len(possible_parents), 4)

        filters_to = {
            "DATES_TO": "20200128,20200129,20200130",
            "MEMBERS_TO": "fc0,fc1",
            "CHUNKS_TO": "1,2,3",
            "SPLITS_TO": "all"
        }
        parent.section = "fake-section-dates"
        possible_parents = jobs_dic.get_jobs_filtered(parent.section, self.mock_job, filters_to, self.mock_job.date,
                                                      self.mock_job.member, 1)
        self.assertEqual(len(possible_parents), 2)
        parent.section = "fake-section-member"
        possible_parents = jobs_dic.get_jobs_filtered(parent.section, self.mock_job, filters_to, self.mock_job.date,
                                                      "fc0", 1)
        self.assertEqual(len(possible_parents), 1)
        parent.section = "fake-section-members"
        possible_parents = jobs_dic.get_jobs_filtered(parent.section, self.mock_job, filters_to, self.mock_job.date,
                                                      "fc0", 1)
        self.assertEqual(len(possible_parents), 2)
        parent.section = "fake-section-single-chunk"
        possible_parents = jobs_dic.get_jobs_filtered(parent.section, self.mock_job, filters_to, self.mock_job.date,
                                                      "fc0", 1)
        self.assertEqual(len(possible_parents), 1)
        parent.section = "fake-section-chunks"
        possible_parents = jobs_dic.get_jobs_filtered(parent.section, self.mock_job, filters_to, self.mock_job.date,
                                                      "fc0", 1)
        self.assertEqual(len(possible_parents), 4)

        filters_to = {
            "DATES_TO": "20200128,20200129,20200130",
            "SPLITS_TO": "all"
        }
        self.mock_job.running = "member"
        self.mock_job.member = "fc0"
        self.mock_job.chunk = 1
        parent.section = "fake-section-member"
        possible_parents = jobs_dic.get_jobs_filtered(parent.section, self.mock_job, filters_to, self.mock_job.date,
                                                      self.mock_job.member, 1)
        self.assertEqual(len(possible_parents), 1)
        parent.section = "fake-section-members"
        possible_parents = jobs_dic.get_jobs_filtered(parent.section, self.mock_job, filters_to, self.mock_job.date,
                                                      "fc0", 1)
        self.assertEqual(len(possible_parents), 2)
        parent.section = "fake-section-chunk"
        possible_parents = jobs_dic.get_jobs_filtered(parent.section, self.mock_job, filters_to, self.mock_job.date,
                                                      "fc0", 1)
        self.assertEqual(len(possible_parents), 1)
        parent.section = "fake-section-chunks"
        possible_parents = jobs_dic.get_jobs_filtered(parent.section, self.mock_job, filters_to, self.mock_job.date,
                                                      "fc0", 1)
        self.assertEqual(len(possible_parents), 2)

        filters_to = {
            "SPLITS_TO": "all"
        }

        parent.section = "fake-section-date"
        parent.date = datetime.strptime("20200128", "%Y%m%d")
        possible_parents = jobs_dic.get_jobs_filtered(parent.section, self.mock_job, filters_to, self.mock_job.date,
                                                      self.mock_job.member, 1)
        self.assertEqual(len(possible_parents), 1)
        parent.section = "fake-section-dates"
        parent.date = datetime.strptime("20200128", "%Y%m%d")
        possible_parents = jobs_dic.get_jobs_filtered(parent.section, self.mock_job, filters_to, self.mock_job.date,
                                                      self.mock_job.member, 1)
        self.assertEqual(len(possible_parents), 2)
        ## Testing parent == once
        # and natural jobs
        self.mock_job.date = datetime.strptime("20200128", "%Y%m%d")
        self.mock_job.member = "fc0"
        self.mock_job.chunk = 1
        self.mock_job.running = "once"
        filters_to = {}
        parent.running = "chunks"
        parent.section = "fake-section-date"
        parent.date = datetime.strptime("20200128", "%Y%m%d")
        possible_parents = jobs_dic.get_jobs_filtered(parent.section, self.mock_job, filters_to, self.mock_job.date,
                                                      self.mock_job.member, 1)
        self.assertEqual(len(possible_parents), 1)
        parent.section = "fake-section-dates"
        parent.date = datetime.strptime("20200128", "%Y%m%d")
        possible_parents = jobs_dic.get_jobs_filtered(parent.section, self.mock_job, filters_to, self.mock_job.date,
                                                      self.mock_job.member, 1)
        self.assertEqual(len(possible_parents), 2)
        parent.section = "fake-section-member"
        parent.date = datetime.strptime("20200128", "%Y%m%d")
        possible_parents = jobs_dic.get_jobs_filtered(parent.section, self.mock_job, filters_to, self.mock_job.date,
                                                      "fc0", 1)
        self.assertEqual(len(possible_parents), 1)
        parent.section = "fake-section-members"
        possible_parents = jobs_dic.get_jobs_filtered(parent.section, self.mock_job, filters_to, self.mock_job.date,
                                                      "fc0", 1)
        self.assertEqual(len(possible_parents), 2)
        parent.section = "fake-section-single-chunk"
        possible_parents = jobs_dic.get_jobs_filtered(parent.section, self.mock_job, filters_to, self.mock_job.date,
                                                      "fc0", 1)
        self.assertEqual(len(possible_parents), 1)
        parent.section = "fake-section-chunks"
        possible_parents = jobs_dic.get_jobs_filtered(parent.section, self.mock_job, filters_to, self.mock_job.date,
                                                      "fc0", 1)
        self.assertEqual(len(possible_parents), 4)

    def test_add_special_conditions(self):
        # Method from job_list
        job = Job("child", 1, Status.READY, 1)
        job.section = "child_one"
        job.date = datetime.strptime("20200128", "%Y%m%d")
        job.member = "fc0"
        job.chunk = 1
        job.split = 1
        job.splits = 1
        job.max_checkpoint_step = 0
        special_conditions = {"STATUS": "RUNNING", "FROM_STEP": "2"}
        only_marked_status = False
        filters_to_apply = {"DATES_TO": "all", "MEMBERS_TO": "all", "CHUNKS_TO": "all", "SPLITS_TO": "all"}
        parent = Job("parent", 1, Status.READY, 1)
        parent.section = "parent_one"
        parent.date = datetime.strptime("20200128", "%Y%m%d")
        parent.member = "fc0"
        parent.chunk = 1
        parent.split = 1
        parent.splits = 1
        parent.max_checkpoint_step = 0
        job.status = Status.READY
        job_list = Mock(wraps=self.JobList)
        job_list._job_list = [job, parent]
        job_list.add_special_conditions(job, special_conditions, only_marked_status, filters_to_apply, parent)
        # self.JobList.jobs_edges
        # job.edges = self.JobList.jobs_edges[job.name]
        # assert
        self.assertEqual(job.max_checkpoint_step, 2)
        value = job.edge_info.get("RUNNING", "").get("parent", ())
        self.assertEqual((value[0].name, value[1]), (parent.name, "2"))
        self.assertEqual(len(job.edge_info.get("RUNNING", "")), 1)

        self.assertEqual(str(job_list.jobs_edges.get("RUNNING", ())), str({job}))
        only_marked_status = False
        parent2 = Job("parent2", 1, Status.READY, 1)
        parent2.section = "parent_two"
        parent2.date = datetime.strptime("20200128", "%Y%m%d")
        parent2.member = "fc0"
        parent2.chunk = 1

        job_list.add_special_conditions(job, special_conditions, only_marked_status, filters_to_apply, parent2)
        value = job.edge_info.get("RUNNING", "").get("parent2", ())
        self.assertEqual(len(job.edge_info.get("RUNNING", "")), 2)
        self.assertEqual((value[0].name, value[1]), (parent2.name, "2"))
        self.assertEqual(str(job_list.jobs_edges.get("RUNNING", ())), str({job}))
        only_marked_status = False
        job_list.add_special_conditions(job, special_conditions, only_marked_status, filters_to_apply, parent2)
        self.assertEqual(len(job.edge_info.get("RUNNING", "")), 2)

if __name__ == '__main__':
    unittest.main()
