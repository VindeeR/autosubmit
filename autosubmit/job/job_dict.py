#!/usr/bin/env python3

# Copyright 2017-2020 Earth Sciences Department, BSC-CNS

# This file is part of Autosubmit.

# Autosubmit is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# Autosubmit is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with Autosubmit.  If not, see <http://www.gnu.org/licenses/>.

from collections.abc import Iterable
import itertools
from contextlib import suppress
from bscearth.utils.date import date2str

from autosubmit.job.job import Job
from autosubmit.job.job_common import Status, Type
from log.log import Log, AutosubmitCritical
from collections import namedtuple

class DicJobs:
    """
    Class to create jobs from conf file and to find jobs by start date, member and chunk

    :param jobs_list: jobs list to use
    :type jobs_list: Joblist

    :param date_list: start dates
    :type date_list: list
    :param member_list: member
    :type member_list: list
    :param chunk_list: chunks
    :type chunk_list: list
    :param date_format: option to format dates
    :type date_format: str
    :param default_retrials: default retrials for ech job
    :type default_retrials: int
    :type default_retrials: config_common
    """

    def __init__(self, date_list, member_list, chunk_list, date_format, default_retrials, jobs_data, experiment_data):
        self._date_list = date_list
        self._member_list = member_list
        self._chunk_list = chunk_list
        self._jobs_data = jobs_data
        self._date_format = date_format
        self.default_retrials = default_retrials
        self._dic = dict()
        self.experiment_data = experiment_data

    def read_section(self, section, priority, default_job_type, jobs_data=dict()):
        """
        Read a section from jobs conf and creates all jobs for it

        :param default_job_type: default type for jobs
        :type default_job_type: str
        :param jobs_data: dictionary containing the plain data from jobs
        :type jobs_data: dict
        :param section: section to read, and it's info
        :type section: tuple(str,dict)
        :param priority: priority for the jobs
        :type priority: int
        """
        parameters = self.experiment_data["JOBS"]

        splits = int(parameters[section].get("SPLITS", -1))
        running = str(parameters[section].get('RUNNING', "once")).lower()
        frequency = int(parameters[section].get("FREQUENCY", 1))
        if running == 'once':
            self._create_jobs_once(section, priority, default_job_type, jobs_data, splits)
        elif running == 'date':
            self._create_jobs_startdate(section, priority, frequency, default_job_type, jobs_data, splits)
        elif running == 'member':
            self._create_jobs_member(section, priority, frequency, default_job_type, jobs_data, splits)
        elif running == 'chunk':
            synchronize = str(parameters[section].get("SYNCHRONIZE", ""))
            delay = int(parameters[section].get("DELAY", -1))
            self._create_jobs_chunk(section, priority, frequency, default_job_type, synchronize, delay, splits,
                                    jobs_data)

    def _create_jobs_startdate(self, section, priority, frequency, default_job_type, jobs_data=dict(), splits=-1):
        """
        Create jobs to be run once per start date

        :param section: section to read
        :type section: str
        :param priority: priority for the jobs
        :type priority: int
        :param frequency: if greater than 1, only creates one job each frequency startdates. Always creates one job
                          for the last
        :type frequency: int
        """
        self._dic[section] = dict()
        count = 0
        for date in self._date_list:
            count += 1
            if count % frequency == 0 or count == len(self._date_list):
                self._dic[section][date] = []
                self._create_jobs_split(splits, section, date, None, None, priority,default_job_type, jobs_data, self._dic[section][date])


    def _create_jobs_member(self, section, priority, frequency, default_job_type, jobs_data=dict(), splits=-1):
        """
        Create jobs to be run once per member

        :param section: section to read
        :type section: str
        :param priority: priority for the jobs
        :type priority: int
        :param frequency: if greater than 1, only creates one job each frequency members. Always creates one job
                          for the last
        :type frequency: int
        :type excluded_members: list
        :param excluded_members: if member index is listed there, the job won't run for this member.

        """
        self._dic[section] = dict()
        for date in self._date_list:
            self._dic[section][date] = dict()
            count = 0
            for member in self._member_list:
                count += 1
                if count % frequency == 0 or count == len(self._member_list):
                    self._dic[section][date][member] = []
                    self._create_jobs_split(splits, section, date, member, None, priority,default_job_type, jobs_data, self._dic[section][date][member])

    def _create_jobs_once(self, section, priority, default_job_type, jobs_data=dict(), splits=0):
        """
        Create jobs to be run once

        :param section: section to read
        :type section: str
        :param priority: priority for the jobs
        :type priority: int
        """
        self._dic[section] = []
        self._create_jobs_split(splits, section, None, None, None, priority, default_job_type, jobs_data,self._dic[section])

    def _create_jobs_chunk(self, section, priority, frequency, default_job_type, synchronize=None, delay=0, splits=0,
                           jobs_data=dict()):
        """
        Create jobs to be run once per chunk

        :param synchronize:
        :param section: section to read
        :type section: str
        :param priority: priority for the jobs
        :type priority: int
        :param frequency: if greater than 1, only creates one job each frequency chunks. Always creates one job
                          for the last
        :type frequency: int
        :param delay: if this parameter is set, the job is only created for the chunks greater than the delay
        :type delay: int
        """
        self._dic[section] = dict()
        # Temporally creation for unified jobs in case of synchronize
        tmp_dic = dict()
        if synchronize is not None and len(str(synchronize)) > 0:
            count = 0
            for chunk in self._chunk_list:
                count += 1
                if delay == -1 or delay < chunk:
                    if count % frequency == 0 or count == len(self._chunk_list):
                        if synchronize == 'date':
                            tmp_dic[chunk] = []
                            self._create_jobs_split(splits, section, None, None, chunk, priority,
                                                    default_job_type, jobs_data, tmp_dic[chunk])
                        elif synchronize == 'member':
                            tmp_dic[chunk] = dict()
                            for date in self._date_list:
                                tmp_dic[chunk][date] = []
                                self._create_jobs_split(splits, section, date, None, chunk, priority,
                                                        default_job_type, jobs_data, tmp_dic[chunk][date])
        # Real dic jobs assignment/creation
        for date in self._date_list:
            self._dic[section][date] = dict()
            for member in self._member_list:
                self._dic[section][date][member] = dict()
                count = 0
                for chunk in self._chunk_list:
                    count += 1
                    if delay == -1 or delay < chunk:
                        if count % frequency == 0 or count == len(self._chunk_list):
                            if synchronize == 'date':
                                if chunk in tmp_dic:
                                    self._dic[section][date][member][chunk] = tmp_dic[chunk]
                            elif synchronize == 'member':
                                if chunk in tmp_dic:
                                    self._dic[section][date][member][chunk] = tmp_dic[chunk][date]
                            else:
                                self._dic[section][date][member][chunk] = []
                                self._create_jobs_split(splits, section, date, member, chunk, priority,
                                                        default_job_type, jobs_data,
                                                        self._dic[section][date][member][chunk])
    def _create_jobs_split(self, splits, section, date, member, chunk, priority, default_job_type, jobs_data, section_data):
        gen = ( job for job in jobs_data.values() if (job[6] == member or member is None) and (job[5] == date or date is None) and (job[7] == chunk or chunk is None) and (job[4] == section or section is None) )
        if splits <= 0:
            self.build_job(section, priority, date, member, chunk, default_job_type, gen, section_data, -1)
        else:
            current_split = 1
            while current_split <= splits:
                self.build_job(section, priority, date, member, chunk, default_job_type, itertools.islice(gen,0,current_split), section_data,current_split)
                current_split += 1
        # clean remaining gen elements if any ( avoids GeneratorExit exception )
        for _ in gen: pass

    def get_jobs(self, section, date=None, member=None, chunk=None):
        """
        Return all the jobs matching section, date, member and chunk provided. If any parameter is none, returns all
        the jobs without checking that parameter value. If a job has one parameter to None, is returned if all the
        others match parameters passed

        :param section: section to return
        :type section: str
        :param date: stardate to return
        :type date: str
        :param member: member to return
        :type member: str
        :param chunk: chunk to return
        :type chunk: int
        :return: jobs matching parameters passed
        :rtype: list
        """
        jobs = list()

        if section not in self._dic:
            return jobs

        dic = self._dic[section]
        # once jobs
        if type(dic) is list:
            jobs = dic
        elif type(dic) is not dict:
            jobs.append(dic)
        else:
            if date is not None and len(str(date)) > 0:
                self._get_date(jobs, dic, date, member, chunk)
            else:
                for d in self._date_list:
                    self._get_date(jobs, dic, d, member, chunk)
        if len(jobs) > 0 and isinstance(jobs[0], list):
            try:
                jobs_flattened = [job for jobs_to_flatten in jobs for job in jobs_to_flatten]
                jobs = jobs_flattened
            except TypeError as e:
                pass
        return jobs

    def _get_date(self, jobs, dic, date, member, chunk):
        if date not in dic:
            return jobs
        dic = dic[date]
        if type(dic) is list:
            for job in dic:
                jobs.append(job)
        elif type(dic) is not dict:
            jobs.append(dic)
        else:
            if member is not None and len(str(member)) > 0:
                self._get_member(jobs, dic, member, chunk)
            else:
                for m in self._member_list:
                    self._get_member(jobs, dic, m, chunk)

        return jobs

    def _get_member(self, jobs, dic, member, chunk):
        if member not in dic:
            return jobs
        dic = dic[member]
        if type(dic) is not dict:
            jobs.append(dic)
        else:
            if chunk is not None and len(str(chunk)) > 0:
                if chunk in dic:
                    jobs.append(dic[chunk])
            else:
                for c in self._chunk_list:
                    if c not in dic:
                        continue
                    jobs.append(dic[c])
        return jobs

    def build_job(self, section, priority, date, member, chunk, default_job_type, jobs_generator,section_data, split=-1):
        name = self.experiment_data.get("DEFAULT", {}).get("EXPID", "")
        if date is not None and len(str(date)) > 0:
            name += "_" + date2str(date, self._date_format)
        if member is not None and len(str(member)) > 0:
            name += "_" + member
        if chunk is not None and len(str(chunk)) > 0:
            name += "_{0}".format(chunk)
        if split > -1:
            name += "_{0}".format(split)
        name += "_" + section
        for job_data in jobs_generator:
            if job_data[0] == name:
                job = Job(job_data[0], job_data[1], job_data[2], priority)
                job.local_logs = (job_data[8], job_data[9])
                job.remote_logs = (job_data[10], job_data[11])
        else:
            job = Job(name, 0, Status.WAITING, priority)

        job.default_job_type = default_job_type
        job.section = section
        job.date = date
        job.member = member
        job.chunk = chunk
        job.split = split
        section_data.append(job)
