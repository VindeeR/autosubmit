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
  name_list = list()
  if self.hasChildren():
   for job in self.children:
    name_list.append(job.getName())
    print name_list
    result_list = job.getAllChildren()
    name_list = name_list+result_list
  return name_list

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
 
 def sefFailCount(self,newfailcount):
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
 
 def CreateJobScript2(self,template,parameters):
  mytemplate=template
  scriptname=self.name+'.cmd'
  splittedname=self.name.split('_')
  #procx=int(splittedname[1])
  parameters['NEMOPROCX']=splittedname[1]
  #procy=int(splittedname[2])
  parameters['NEMOPROCY']=splittedname[2]
  parameters['JOBNAME'] = self.name 
  ##update parameters
  print "*************My Template: %s" % mytemplate
  templateContent = file(mytemplate).read()
  for key in parameters.keys():
   print "KEY: %s\n" % key
   if key in templateContent:
    print "%s:\t%s" % (key,parameters[key])
    templateContent = templateContent.replace(key,parameters[key])
  
  file(scriptname,'w').write(templateContent)
  return scriptname


 def CreateJobScript(self,template,parameters):
  scriptname=self.name+'.cmd'
  splittedname=scriptname.split('_')
  #date=int(splittedname[1])
  parameters['DATE']=splittedname[1]
  #member=int(splittedname[2])
  parameters['MEMBER']=splittedname[2]
  chunk=int(splittedname[3])
  parameters['CHUNK']=splittedname[3]
  parameters['JOBNAME'] = self.name 
  prev=[0,59,59+61,59+61*2,59+61*2+62,59+61*3+62]
  if (self.jobtype==0):
   print "jobType:", self.JobType
   mytemplate=template+'.sim'
   ##update parameters
   parameters['WALLCLOCKLIMIT']='72:00:00'
   parameters['PREV']=str(24*prev[chunk-1])

  elif (self.jobtype==1):
   print "jobType:", self.JobType
   mytemplate=template+'.post'
   ##update parameters
   parameters['WALLCLOCKLIMIT']="02:01:00"
  elif (self.jobtype==2):
   print "jobType:", self.JobType
   ##update parameters
   mytemplate=template+'.clean'
   parameters['WALLCLOCKLIMIT']="10:00:00"
  else: 
   print "Unknown Job Type"
   
  print "*************My Template: %s" % mytemplate
  templateContent = file(mytemplate).read()
  for key in parameters.keys():
   print "KEY: %s\n" % key
   if key in templateContent:
    print "%s:\t%s" % (key,parameters[key])
    templateContent = templateContent.replace(key,parameters[key])
  
  file(scriptname,'w').write(templateContent)
  return scriptname

 def check_completion(self):
  complete=False
  if (self.jobtype==0):
   #check that the simulation finished properly
   os.system('touch %s' % self.name+'COMPLETED')
   complete=True
  elif (self.jobtype==1):
   #check that the postprocessing finished properly
   os.system('touch %s' % self.name+'COMPLETED')
   complete=True
  elif (self.jobtype==2):
   #check that the cleaning finished properly
   os.system('touch %s' % self.name+'COMPLETED')
   complete=True
  else:
   os.system('touch %s' % self.name+'COMPLETED')
   complete=True
   
  if (complete):
   self.setStatus(Job.Status.COMPLETED)
   if (self.hasChildren()!=0):
    children=self.getChildren()
    print "Job is completed, we are now removing the dependency in his %s child/children:" % self.hasChildren()
    for child in children:
     #print "child type.......:",type(child)
     #self.printJob()
     #child.printJob()
     #print "number of Parents:",child.hasParents()
     #print "\n really???"
     #for parent in child.getParents()
     # parent.printJob()
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
