#!/usr/bin/env python

import commands
import xml.dom.minidom as xml


# Tags into xml mnq file
JOB = 'job'
JOBNAME = 'JobName'
JOBID = 'JobID'
STATE = 'State'
USER = 'User'
REQPROCS = 'ReqProcs'
MASTERHOST = 'MasterHost'
TSUB = 'SubmissionTime'
duration1 = 'AWDuration'
duration2 = 'EEDuration'
duration3 = 'ReqAWDuration'
STIME = 'StartTime'
rsvSTIME= 'RsvStartTime'
def createTable(xmlDoc):
 # Job table declaration
 jobTable = {}
 for job in xmlDoc.getElementsByTagName(JOB):
  jobName = job.getAttribute(JOBNAME).encode().replace('"','')
  jobId = job.getAttribute(JOBID).encode()
  jobStatus = job.getAttribute(STATE).encode()
  jobUser = job.getAttribute(USER).encode()
  jobProcs = job.getAttribute(REQPROCS).encode()
  jobMaster = job.getAttribute(MASTERHOST).encode()
  jobTsub = job.getAttribute(TSUB).encode()
  job1 = job.getAttribute(duration1).encode()
  job2 = job.getAttribute(duration2).encode()
  job3 = job.getAttribute(duration3).encode()
  stime = job.getAttribute(STIME).encode()
  rsvstime = job.getAttribute(rsvSTIME).encode()
  jobTable[jobName] = (jobId,jobStatus,jobUser,jobProcs,jobMaster,jobTsub,job1,job2,job3,stime,rsvstime)
 return jobTable

def getAllJobs():
 """Method for retrieving a group job list with different attributes.
    This function returns a table {jobId: values}"""
 output = commands.getoutput("ssh mn 'mnq --xml -w group=ecm86'")
 file('/var/tmp/mnq.xml','w').write(output.strip())
 docParsed = xml.parse('/var/tmp/mnq.xml')
 return createTable(docParsed)

def getMyJobs():
 """Method for retrieving a user job list with different attributes.
    This function returns a table {jobId: values}"""
 output = commands.getoutput("ssh mn 'mnq --xml'")
 file('/var/tmp/user_mnq.xml','w').write(output.strip())
 docParsed = xml.parse('/var/tmp/user_mnq.xml')
 return createTable(docParsed)

def getId(jobName):
 table = getAllJobs()
 job = table.get(jobName)
 if job is not None:
  return job[0]
 return 'Name not found!!!'

def getStatus(jobName):
 table = getAllJobs()
 job = table.get(jobName)
 if job is not None:
  return job[1]
 return 'Name not found!!!'

def getUser(jobName):
 table = getAllJobs()
 job = table.get(jobName)
 if job is not None:
  return job[2]
 return 'Name not found!!!'

def getProcs(jobName):
 table = getAllJobs()
 job = table.get(jobName)
 if job is not None:
  return job[3]
 return 'Name not found!!!'

def getMaster(jobName):
 table = getAllJobs()
 job = table.get(jobName)
 if job is not None:
  return job[4]
 return 'Name not found!!!'

def main():
 table = getAllJobs()
 print table
 table = getMyJobs()
 print table
 
 table = getAllJobs()
 if table.__len__() is not 0:
  jobName = table.keys()[0]
 else:
  jobName = 'whatever'

 print "%-12s:%s" % (JOBNAME,jobName)
 print "%-12s:%s" % (JOBID,getId(jobName))
 print "%-12s:%s" % (STATE,getStatus(jobName))
 print "%-12s:%s" % (USER,getUser(jobName))
 print "%-12s:%s" % (REQPROCS,getProcs(jobName))
 print "%-12s:%s" % (MASTERHOST,getMaster(jobName))

 attlist=('JobID','State','User','ReqProcs','MasterHost','SubmissionTime','AWDuration','EEDuration','ReqAWDuration','StartTime','RsvStartTime')
 job = table.get(jobName)
 for i in range(len(attlist)):
  print attlist[i],job[i]

if __name__ == "__main__":
 main()
