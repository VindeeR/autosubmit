#!/usr/bin/env python

import commands, os
import xml.dom.minidom as xml


# Tags into xml mnq file
JOB = 'job'
JOBNAME = 'JobName'
JOBID = 'JobID'
STATE = 'State'
USER = 'User'
REQPROCS = 'ReqProcs'
MASTERHOST = 'MasterHost'

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
  jobTable[jobName] = (jobId,jobStatus,jobUser,jobProcs,jobMaster)
 return jobTable

def getAllJobs():
 """Method for retrieving a group job list with different attributes.
    This function returns a table {jobId: values}"""
 output = commands.getoutput('mnq --xml -w group=ecm86')
 #output = commands.getoutput('cat mnq.xml')
 file('/var/tmp/mnq.xml','w').write(output.strip())
 docParsed = xml.parse('/var/tmp/mnq.xml')
 return createTable(docParsed)

def getMyJobs():
 """Method for retrieving a user job list with different attributes.
    This function returns a table {jobId: values}"""
 #output = commands.getoutput('mnq --xml')
 output = commands.getoutput('cat mnq.xml')
 #file('/var/tmp/mnq.xml','w').write(output.strip())
 file('mnq.xml','w').write(output.strip())
 #docParsed = xml.parse('/var/tmp/mnq.xml')
 docParsed = xml.parse('mnq.xml')
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

if __name__ == "__main__":
 table = getAllJobs()
 print table
 table = getMyJobs()
 print table
 
 print

 #jobName = 'b002_fc0_19601101'
 table = getAllJobs()
 if table.__len__() is not 0:
  jobName = table.keys()[0]
 else:
  jobName = 'pepe'

 print "%-12s:%s" % (JOBNAME,jobName)
 print "%-12s:%s" % (JOBID,getId(jobName))
 print "%-12s:%s" % (STATE,getStatus(jobName))
 print "%-12s:%s" % (USER,getUser(jobName))
 print "%-12s:%s" % (REQPROCS,getProcs(jobName))
 print "%-12s:%s" % (MASTERHOST,getMaster(jobName))
