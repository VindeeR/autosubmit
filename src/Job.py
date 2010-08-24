#!/usr/bin/env python

import os

class Job:
 """Class to handle all the tasks with Jobs at HPC"""
 class Status:
  WAITING = 0
  READY = 1
  SUBMITTED = 2 
  QUEUING = 3
  RUNNING = 4
  COMPLETED = 5
  FAILED = -1

 class JobType:
  SIMULATION = 0
  POSTPROCESSING = 1
  CLEANING = 2
  INITIALISATION = -1
  
 def __init__(self,name,id,status,jobtype):
  self.name = name
  self.id = id
  self.status = status
  self.jobtype = jobtype
  self.parents = list()
  self.children = list()
  self.failcount=0
 #self.parameters = dict()
  
 def getName(self):
  return self.name
 
 def getId(self):
  return self.id
 
 def getStatus(self):
  return self.status
 
 def getJobType(self):
  return self.jobtype

 def getParents(self):
  return self.parents

 def getChildren(self):
  return self.children
 
 def getAllChildren(self):
  print self.name
  job_list = list()
  if self.hasChildren():
   for job in self.children:
    job_list.append(job)
    print job.getName()
    result_list = job.getAllChildren()
    job_list = job_list+result_list
  return job_list

 def printJob(self):
  print "%s\t%s\t%s" % ("Job Name","Job Id","Job Status")
  print "%s\t\t%s\t%s" % (self.name,self.id,self.status)

 def getFailCount(self):
  return self.failcount
 
 def setName(self,newName):
  self.name = newName
 
 def setId(self,newId):
  self.id = newId
 
 def setStatus(self,newStatus):
  self.status = newStatus

 def setJobType(self,newtype):
  self.jobtype = newtype

 def setParents(self,newParents):
  self.parents = newParents
 
 def setChildren(self,newChildren):
  self.children = newChildren
 
 def setFailCount(self,newfailcount):
  self.failcount=newfailcount
 
 def addParents(self,newParents):
  self.parents += [newParents]
 
 def addChildren(self,newChildren):
  self.children += [newChildren]

 def deleteParent(self,parent):
  #careful, it is only possible to remove one parent at a time 
  self.parents.remove(parent)
 
 def deleteChild(self,child):
  # careful it is only possible to remove one child at a time
  self.children.remove(child)

 def hasChildren(self):
  return self.children.__len__() 

 def hasParents(self):
  return self.parents.__len__() 


 def check_completion(self):
  complete=False
  logname='mylogs/'+self.name+'_COMPLETED'
  if  (os.path.exists(logname)):
   complete=True
   os.system('touch %s' % self.name+'Checked')
  else:
   print "Job: %s has failed!!!" % self.name
   os.system('touch %s' % self.name+'Failed')
   
  if (complete):
   self.setStatus(Job.Status.COMPLETED)
   if (self.hasChildren()!=0):
    children=self.getChildren()
    print "Job is completed, we are now removing the dependency in his %s child/children:" % self.hasChildren()
    for child in children:
     #print "child type.......:",type(child)
     #self.printJob()
     #child.printJob()
     print "number of Parents:",child.hasParents()
     print "\n really???"
     for parent in child.getParents():
      parent.printJob()
     child.deleteParent(self)

  else:
   print "The checking in check_completion tell us that job %s has failed" % self.name
   self.setStatus(Job.Status.FAILED)


if __name__ == "__main__":
 mainJob = Job('uno','1',Job.Status.READY,0)
 job1 = Job('uno','1',Job.Status.READY,0)
 job2 = Job('dos','2',Job.Status.READY,0)
 job3 = Job('tres','3',Job.Status.READY,0)
 jobs = [job1,job2,job3]
 mainJob.setParents(jobs)
 print mainJob.getParents()
 mainJob.setChildren(jobs)
 print mainJob.getChildren()
 job3.check_completion() 
 print "Number of Parents: ", mainJob.hasParents()
 print "number of children : ", mainJob.hasChildren()
 mainJob.printJob()
