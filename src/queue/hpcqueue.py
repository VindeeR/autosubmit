#!/usr/bin/env python

from commands import getstatusoutput
from time import sleep

UNKNOWN = 0
RUNNING = 1

class HPCQueue:
	def cancel_job(self, job_id):
		print 'ssh '+self._host+' "'+self._cancel_cmd+' ' + str(job_id) + '"'
		(status, output) = getstatusoutput('ssh '+self._host+' "'+self._cancel_cmd+' ' + str(job_id) + '"')
	
	def check_job(self, job_id):
		job_status = UNKNOWN

		if type(job_id) is not int:
			# URi: logger
			print('check_job() The argument %s is not an integer.' % job_id)
			# URi: value ?
			return job_status 

		retry = 10;
		(status, output) = getstatusoutput('ssh '+self._host+' "'+self._checkjob_cmd+' %s"' % str(job_id))
		while(status!=0 and retry>0):
			retry -= 1
			(status, output) = getstatusoutput('ssh '+self._host+' "'+self._checkjob_cmd+' %s"' % str(job_id))
			# URi: logger
			print('Can not get job status, retrying in 5 sec\n');
			sleep(5)

		if(status == 0):
			# URi: this command is specific of mn
			# job_status = output[1:-1].split('\n').[3].split(' ')[1]
			job_status = self.parse_job_output(output)
			# URi: define status list in HPC Queue Class
			if (job_status in self._job_status['COMPLETED'] or retry == 0):
				job_status = COMPLETED
			elif (job_status in self._job_status['RUNNING']):
				job_status = RUNNING
			elif (job_status in self._job_status['QUEUING']):
				job_status = QUEUING
			elif (job_status in self._job_status['FAILED']):
				job_status = FAILED
			else:
				job_status = UNKNOWN

		return job_status

	def submit_job(self, job_script):
		(status, output) = getstatusoutput('ssh '+self._host+' "'+self._submit_cmd+' ' + self._pathdir+'/'+str(job_script) + '"')

		if(status == 0):
			job_id = self.get_submitted_job_id(output)
			return int(job_id)

