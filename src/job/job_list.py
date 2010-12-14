#!/usr/bin/env python

from job_common import Status
from job_common import Type
from job import Job
import os
import cPickle as pickle

class JobList:
	
	def __init__(self, expid, date_list, member_list, starting_chunk, num_chunks):
		self.path = "../auxfiles/"
		self.update_file = "updated_job_list_" + expid + ".pkl"
		self.failed_file = "failed_job_list_" + expid + ".pkl"
		self.job_list = list()

		for date in date_list:
			for member in member_list:
				for	chunk in range(starting_chunk, starting_chunk + num_chunks+1):
					rootjob_name = "job_" + str(date) + "_" + str(member) + "_" + str(chunk) + "_"
					post_job = Job(rootjob_name+"post", 0, Status.WAITING, Type.POSTPROCESSING)
					clean_job = Job(rootjob_name+"clean", 0, Status.WAITING, Type.CLEANING)
					if (starting_chunk == 1):
						sim_job = Job(rootjob_name+"sim", 0, Status.WAITING, Type.SIMULATION)
					else:
						sim_job = Job(rootjob_name+"sim", 0, Status.READY, Type.SIMULATION)

					# set dependency of postprocessing jobs
					post_job.set_parents([sim_job.get_name()])
					post_job.set_children([clean_job.get_name()])
					# set parents of clean job
					clean_job.set_parents([post_job.get_name()])
					# set first child of simulation job
					sim_job.set_children([post_job.get_name()])
					# set status of first chunk to READY
					if (chunk > 1):
						parentjob_name = "job_" + str(date) + "_" + str(member) + "_" + str(chunk-1) + "_" + "sim"
						sim_job.set_parents([parentjob_name])
						if (chunk > 2):
							parentjob_name = "job_" + str(date) + "_" + str(member) + "_" + str(chunk-2) + "_" + "clean"
							sim_job.set_parents([parentjob_name])
					elif (chunk == 1):
						init_job = Job(rootjob_name + "init", 0, Status.READY,Type.INITIALISATION)
						init_job.set_children([sim_job.get_name()])
						init_job.set_parents([])
						sim_job.set_parents([init_job.get_name()])
						self.job_list += [init_job]
					elif (chunk < starting_chunk + num_chunks):
						childjob_name = "job_" + str(date) + "_" + str(member) + "_" + str(chunk+1) + "_" + "sim"
						sim_job.add_children(childjob_name)
					elif (chunk < starting_chunk + num_chunks - 1):
						childjob_name = "job_" + str(date) + "_" + str(member) + "_" + str(chunk+2) + "_" + "sim"
						clean_job.set_children([childjob_name])

					self.job_list += [sim_job, post_job, clean_job]

		self.update_genealogy()

	def get_completed(self):
		"""Returns a list of completed jobs"""
		return [job for job in self.job_list if job.get_status() == Status.COMPLETED]

	def get_submitted(self):
		"""Returns a list of submitted jobs"""
		return [job for job in self.job_list if job.get_status() == Status.SUBMITTED]

	def get_running(self):
		"""Returns a list of jobs running"""
		return [job for job in self.job_list if job.get_status() == Status.RUNNING]

	def get_queuing(self):
		"""Returns a list of jobs queuing"""
		return [job for job in self.job_list if job.get_status() == Status.QUEUING]

	def get_failed(self):
		"""Returns a list of failed jobs"""
		return [job for job in self.job_list if job.get_status() == Status.FAILED]

	def get_ready(self):
		"""Returns a list of jobs ready"""
		return [job for job in self.job_list if job.get_status() == Status.READY]

	def get_waiting(self):
		"""Returns a list of jobs waiting"""
		return [job for job in self.job_list if job.get_status() == Status.WAITING]

	def get_in_queue(self):
		"""Returns a list of jobs in the queue (Submitted, Running, Queuing)"""
		return self.get_submitted() + self.get_running() + self.get_queuing()

	def get_not_in_queue(self):
		"""Returns a list of jobs NOT in the queue (Ready, Waiting)"""
		return self.get_ready() + self.get_waiting()

	def get_finished(self):
		"""Returns a list of jobs finished (Completed, Failed)"""
		return self.get_completed() + self.get_failed()

	def get_active(self):
		"""Returns a list of active jobs (In queue, Ready)"""
		return self.get_in_queue() + self.get_ready()
	
	def get_job_by_name(self, name):
		"""Returns the job that its name matches name"""
		for job in self.job_list:
			if job.get_name() == name:
				return job
		return False
	
	def sort_by_name(self):
		return sorted(self.job_list, key=lambda k:k.get_name())
	
	def sort_by_id(self):
		return sorted(self.job_list, key=lambda k:k.get_id())

	def sort_by_type(self):
		return sorted(self.job_list, key=lambda k:k.get_type())
	
	def sort_by_status(self):
		return sorted(self.job_list, key=lambda k:k.get_status())
	
	def load_file(self, filename):
		if(os.path.exists(filename)):
			return pickle.load(file(filename, 'r'))
		else
			# URi: print ERROR
			return list() 
	
	def load_updated(self):
		return self.load_file(self.path + self.update_file)

	def load_failed(self):
		return self.load_file(self.path + self.failed_file)

	def save_failed(self):
		# URi: should we check that the path exists?
		pickle.dump(self.job_list, file(self.path + self.failed_file, 'w'))

	def update_list(self):
		#load updated file list
		updated_list = self.load_updated()
		self.job_list += self.updated_list
		# recover failed list
		failed_list = self.load_failed()
		# remove elements that are already in the job_list, may be because they have been updated
		for failed_job in failed_list:
			if self.get_job_by_name(failed_job.get_name()):
				failed_list.remove(failed_job)
		# reset jobs that has failed less than 4 times
		for job in self.get_failed():
			job.inc_fail_count()
			if job.get_fail_count < 4:
				job.set_status(Status.READY)
			else:
				# get all childrens of a job that has failed more than 3 times
				child_list = job.get_all_children()
				# add job to the failed list
				failed += [job]
				# remove job from the "working" list
				self.job_list.remove(job)
				# add to the failed list all childrens of the failed job
				failed.append(child_list)
				# remove all childrens of the failed job from the "working" list
				for child in child_list:
					self.job_list.remove(child)
				self.save_failed()

		
		# if waiting jobs has all parents completed change its State to READY
		for job in self.get_waiting():
			for parent in job.get_parents():
				if parent.get_status() != Status.COMPLETED:
					break	
			job.set_status(Status.READY)

	def update_genealogy(self):
		"""When we have create the joblist, parents and child list just contain the names. Update the genealogy replacing job names by the corresponding job object"""
		for job in self.job_list:
			if job.has_children():
				# get the list of childrens (names)
				child_list = job.get_children()
				# remove the list of names
				job.set_children([])
				# for each child find the corresponding job
				for child in child_list:
					if isinstance(child, str):
						job_object = self.get_job_by_name(child)
						job.add_children(job_object)
					else:
						job.add_children(child)

			if job.has_parents():
				# get the list of childrens (names)
				parent_list = job.get_parents()
				# remove the list of names
				job.set_parents([])
				# for each child find the corresponding job
				for parent in parent_list:
					if isinstance(parent, str):
						job_object = self.get_job_by_name(parent)
						job.add_parents(job_object)
					else:
						job.add_parents(parent)

