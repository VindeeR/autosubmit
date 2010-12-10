#!/usr/bin/env python

from job_common import Status
from job_common import Type
from job import Job

class JobList:
	
	def __init__(self, date_list, member_list, starting_chunk, num_chunks):
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
							parentjob_name = "job_" + str(date) + "_" + str(member) + "_" + str(chunk-1) + "_" + "clean"
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
						sim_job.set_children([childjob_name])

					self.job_list += [sim_job, post_job, clean_job]



