#!/usr/bin/env python

from commands import getstatusoutput
from time import sleep
from job.job_common import Status

class HPCQueue:
	def cancel_job(self, job_id):
		print 'ssh ' + self._host + ' "' + self._cancel_cmd + ' ' + str(job_id) + '"'
		(status, output) = getstatusoutput('ssh '+self._host+' "'+self._cancel_cmd+' ' + str(job_id) + '"')
	
	def check_job(self, job_id):
		job_status = Status.UNKNOWN

		if type(job_id) is not int:
			# URi: logger
			print('check_job() The argument %s is not an integer.' % job_id)
			# URi: value ?
			return job_status 

		retry = 10;
		(status, output) = getstatusoutput('ssh ' + self._host + ' "' + self._checkjob_cmd + ' %s"' % str(job_id))
		print	'ssh '+self._host+' "'+self._checkjob_cmd+' %s"' % str(job_id)
		print status
		print output
		while(status!=0 and retry>0):
			retry -= 1
			(status, output) = getstatusoutput('ssh ' + self._host + ' "' + self._checkjob_cmd + ' %s"' % str(job_id))
			print status
			# URi: logger
			print('Can not get job status, retrying in 5 sec\n');
			sleep(5)

		if(status == 0):
			# URi: this command is specific of mn
			job_status = self.parse_job_output(output)
			# URi: define status list in HPC Queue Class
			if (job_status in self._job_status['COMPLETED'] or retry == 0):
				job_status = Status.COMPLETED
			elif (job_status in self._job_status['RUNNING']):
				job_status = Status.RUNNING
			elif (job_status in self._job_status['QUEUING']):
				job_status = Status.QUEUING
			elif (job_status in self._job_status['FAILED']):
				job_status = Status.FAILED
			else:
				job_status = Status.UNKNOWN

		return job_status
	
	def	check_pathdir(self):
		(status, output) = getstatusoutput('ssh '+self._host+' "mkdir	-p	' + self._pathdir + '"')
		if	status	==	0:
			print '%s has been created on %s .' %(self._pathdir, self._host)
		else:
			print 'Could not create the DIR on HPC' 
	
	def	send_script(self,job_script):
		(status, output) = getstatusoutput('scp ../tmp/' + str(job_script) + '	' + self._host + ':' + self._pathdir + '/')
		if(status == 0):
   			print 'The script has been sent'
		else:	
			print 'the script has not been sent'
	
	def submit_job(self, job_script):
		(status, output) = getstatusoutput('ssh ' + self._host + ' "' + self._submit_cmd +' ' + self._pathdir + '/' + str(job_script) + '"')
		print 'ssh ' + self._host + ' "' + self._submit_cmd + ' ' + self._pathdir + '/' + str(job_script)
		if(status == 0):
			job_id = self.get_submitted_job_id(output)
			print job_id
			return int(job_id)

