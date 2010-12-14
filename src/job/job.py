#!/usr/bin/env python
import os
from job_common import Status
from job_common import Type

class Job:
	"""Class to handle all the tasks with Jobs at HPC.
	   A job is created by default with a name, a jobid, a status and a type.
	   It can have children and parents. The inheritance reflects the dependency between jobs.
	   If Job2 must wait until Job1 is completed then Job2 is a 'child of Job1. Inversely Job1 is a parent of Job2 """
  
	def __init__(self, name, id, status, jobtype):
		self._name = name
		self._id = id
		self._status = status
		self._type = jobtype
		self._parents = list()
		self._children = list()
		self._failcount=0
		self._expid = ''
		self._complete = True
 
	def print_job(self):
		print 'NAME: %s' %self._name 
		print 'JOBID: %s' %self._id 
		print 'STATUS: %s' %self._status
		print 'TYPE: %s' %self._type
		print 'PARENTS: %s' %self._parents
		print 'CHILDREN: %s' %self._children
		print 'FAILCOUNTS: %s' %self._failcount
		print 'EXPID: %s' %self._expid 
 
 
	def get_name(self):
		"""Returns the job name"""
		return self._name
 
	def get_id(self):
		"""Returns the job id"""
		return self._id
 
	def get_status(self):
		"""Returns the job status"""
		return self._status
 
	def get_type(self):
		"""Returns the job type"""
		return self._type

	def get_expid(self):
		return self._expid
 
	def get_parents(self):
		"""Returns a list with job's parents"""
		return self._parents

	def get_children(self):
		"""Returns a list with job's childrens"""
		return self._children

	def log_job(self):
		job_logger.info("%s\t%s\t%s" % ("Job Name","Job Id","Job Status"))
		job_logger.info("%s\t\t%s\t%s" % (self.name,self.id,self.status))

	def get_all_children(self):
		"""Returns a list with job's childrens and all it's descendents"""
		job_list = list()
		for job in self._children:
			job_list.append(job)
			job_list += job.get_all_children()
		return job_list

	def get_fail_count(self):
		return self._fail_count
 
	def set_name(self, newName):
		self._name = newName
 
	def set_id(self, new_id):
		self._id = new_id
 
	def set_status(self, new_status):
		self._status = new_status
  
	def set_expid(self, new_expid):
		self._expid = new_expid

	def set_type(self, new_type):
		self._type = new_type

	def set_parents(self, new_parents):
		self._parents = new_parents
 
	def set_children(self, new_children):
		self._children = new_children
 
	def set_fail_count(self, new_fail_count):
		self._failcount = new_fail_count
	
	def inc_fail_count(self):
		self._failcount += 1
 
	def add_parents(self, new_parents):
		self._parents += [new_parents]
 
	def add_children(self, new_children):
		self._children += [new_children]

	def delete_parent(self, parent):
		# careful, it is only possible to remove one parent at a time 
		self._parents.remove(parent)
 
	def delete_child(self, child):
		# careful it is only possible to remove one child at a time
		self._children.remove(child)

	def has_children(self):
		return self._children.__len__() 

	def has_parents(self):
		return self._parents.__len__() 

	def check_completion(self):
		logname='../tmp/'+self._name+'_COMPLETED'
		if(os.path.exists(logname)):
			self._complete=True
			os.system('touch ../tmp/%s' % self._name+'Checked')
		else:
			os.system('touch ../tmp/%s' % self._name+'Failed')
   
	def remove_dependencies(self):
		if (self._complete):
			self.set_status(Status.COMPLETED)
			#job_logger.info("Job is completed, we are now removing the dependency in his %s child/children:" % self.has_children())
			for child in self.get_children():
				#job_logger.debug("number of Parents:", child.has_parents())
				if child.get_parents().__contains__(self):
					child.delete_parent(self)
		else:
			#job_logger.info("The checking in check_completion tell us that job %s has failed" % self.name)
			self.set_status(Status.FAILED)


if __name__ == "__main__":
	mainJob = Job('uno','1',Status.READY,0)
	job1 = Job('uno','1',Status.READY,0)
	job2 = Job('dos','2',Status.READY,0)
	job3 = Job('tres','3',Status.READY,0)
	jobs = [job1,job2,job3]
	mainJob.set_parents(jobs)
	print mainJob.get_parents()
	#mainJob.set_children(jobs)
	job1.add_children(mainJob)
	job2.add_children(mainJob)
	job3.add_children(mainJob)
	print mainJob.get_all_children();
	print mainJob.get_children()
	job3.check_completion() 
	print "Number of Parents: ", mainJob.has_parents()
	print "number of children : ", mainJob.has_children()
	mainJob.print_job()
