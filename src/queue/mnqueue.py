#!/usr/bin/env python

from xml.dom.minidom import parseString
from hpcqueue import HPCQueue
from time import sleep

class MnQueue(HPCQueue):
	def __init__(self,expid):
		self._host = "mn"
		self._cancel_cmd = "mncancel"
		self._checkjob_cmd = "checkjob --xml"
		self._submit_cmd = "mnsubmit"
		self._job_status = dict()
		self._job_status['COMPLETED'] = ['Completed']
		self._job_status['RUNNING'] = ['Running']
		self._job_status['QUEUING'] = ['Pending', 'Idle', 'Blocked']
		self._job_status['FAILED'] = ['Failed', 'Node_fail', 'Timeout']
		self._pathdir = "\$HOME/LOG_"+expid
	
	def parse_job_output(self, output):
		dom = parseString(output)
		job_xml = dom.getElementsByTagName("job")
		job_state = job_xml[0].getAttribute('State')
		return job_state

	def get_submitted_job_id(self, output):
		return output.split(' ')[3]
