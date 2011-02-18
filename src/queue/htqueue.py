#!/usr/bin/env python

from xml.dom.minidom import parseString
from hpcqueue import HPCQueue
from time import sleep

class HtQueue(HPCQueue):
	def __init__(self,expid):
		self._host = "hector"
		self._cancel_cmd = "qdel"
		self._checkjob_cmd = "qstat"
		self._submit_cmd = "qsub"
		self._status_cmd = "qstat -u \$USER |tail -n +6|cut -d' ' -f1"
		self._job_status = dict()
		self._job_status['COMPLETED'] = ['F']
		self._job_status['RUNNING'] = ['R']
		self._job_status['QUEUING'] = ['Q', 'H', 'S']
		self._job_status['FAILED'] = ['Failed', 'Node_fail', 'Timeout']
		self._pathdir = "\$HOME/LOG_"+expid
	
	def parse_job_output(self, output):
		job_state = output.split('\n')[2].split()[4]
		return job_state

	def get_submitted_job_id(self, output):
		return output.split('.')[0]
	
	def jobs_in_queue(self,	output):
		print output
		return output.split()
