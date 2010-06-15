#!/usr/bin/env python

import pickle
from Job import *


def compareStatus(job_a,job_b):
 return cmp(int(job_a.getStatus()),int(job_b.getStatus()))

def compareId(job_a,job_b):
 return cmp(int(job_a.getId()),int(job_b.getId()))

def compareName(job_a,job_b):
 return cmp(job_a.getName(),job_b.getName())

def sortByName(jobs):
 return sorted(jobs,compareName)

def getName(jobs,name):
 for job in jobs:
  if(job.getName()==name):
   return job

def printJobs(jobs):
 print "%s\t%s\t%s" % ("Job Name","Job Id","Job Status")
 for job in jobs:
  print "%s\t\t%s\t%s" % (job.getName(),job.getId(),job.getStatus())

def getCompleted(jobs):
 jobcompleted=[job for job in jobs if job.getStatus()==5]
 return jobcompleted

def getSubmitted(jobs):
 jobsubmitted=[job for job in jobs if job.getStatus()==2]
 return jobsubmitted

def getRunning(jobs):
 jobl=[job for job in jobs if job.getStatus()==4]
 return jobl

def getQueuing(jobs):
 jobl=[job for job in jobs if job.getStatus()==3]
 return jobl

def getFailed(jobs):
 jobl=[job for job in jobs if job.getStatus()==-1]
 return jobl
 
def getReady(jobs):
 jobl=[job for job in jobs if job.getStatus()==1]
 return jobl

def getWaiting(jobs):
 jobl=[job for job in jobs if job.getStatus()==0]
 return jobl

def getInQueue(jobs):
 jobl=getQueuing(jobs)+getRunning(jobs)+getReady(jobs)
 return jobl

def getFinished(jobs):
 jobl=getCompleted(jobs)+getFailed(jobs)
 return jobl

def getActive(jobs):
 jobl=getInQueue(jobs)+getWaiting(jobs)
 return jobl

def sortById(jobs):
 return sorted(jobs,compareId)

def sortByStatus(jobs):
 return sorted(jobs,compareStatus)

def updateJobList(jobs):
 print "*******************UPDATING THE LIST****************************"
 for job in jobs:
  if (job.getStatus()==-1):
   count=job.getFailCount()
   job.setFailCount(count+1)
   if (job.getFailCount()<4):
    job.setStatus(Job.Status.READY)
   else:
    print "Job %s has failed 4 times" % job.getName()
    children=job.getAllChildren()
    print " Now failing all of its heirs..."
    printJobs(children)
    for child in children:
     child.setStatus(Job.Status.FAILED)
     child.setFailCount(4)
  elif job.getStatus()==0 and job.hasParents()==0:
   job.setStatus(Job.Status.READY)

def main():
 job1 = Job('one','1',Job.Status.RUNNING,0)
 job2 = Job('two','2',Job.Status.READY,0)
 job3 = Job('three','3',Job.Status.COMPLETED,0)
 job4 = Job('four','4',Job.Status.READY,0)
 job5 = Job('five','5',Job.Status.READY,0)
 job6 = Job('six','6',Job.Status.READY,0)
 job7 = Job('seven','7',Job.Status.WAITING,0)
 job8 = Job('eight','8',Job.Status.WAITING,0)
 job1.setChildren([job7])
 job3.setChildren([job8])
 jobs = [job1,job2,job3,job4,job5,job6,job7,job8]
 return jobs

def updateGenealogy(jobs):
 print "in genealogy!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
 for job in jobs:
  job.printJob()
  if(job.hasChildren()!=0):
   ##print "number of Children:",job.hasChildren()
   children=job.getChildren()
   ##print children
   #reset job.children list
   job.setChildren([])
   for child in children:
    if isinstance(child,str):
     jobchild=getName(jobs,child)
     ##print "childname %s has type:" % child, type(jobchild)
     job.addChildren(jobchild)
    else:
     ##print "surely child has already the type job:",child
     child.printJob()
     job.addChildren(child)
   
  if(job.hasParents()!=0):
   ##print "Number of Parents:",job.hasParents()
   parents=job.getParents()
   ##print parents
   #reset job.children list
   job.setParents([])
   for parent in parents:
    if isinstance(parent,str):
     ##print "parentname %s has type:" % parent, type(parent)
     jobparent=getName(jobs,parent)
     job.addParents(jobparent)
    else:
     ##print "surely parent has already a type job",parent
     parent.printJob()
     job.addParents(parent)
 print "after genealogy!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"

def CreateJobList(list_of_dates, num_member,num_chunks):
 joblist=list()
 #param=dict()
 for dates in list_of_dates:
  for mem in range(num_member):
   for chk in range(1,num_chunks+1):
    job_rootname="job_"+str(dates)+'_'+str(mem)+'_'+str(chk)+'_'
    job_sim = Job(job_rootname+'sim',0,Job.Status.WAITING,Job.JobType.SIMULATION)
    #job_sim.CreateParametersList(parameters)
    job_post = Job(job_rootname+'post',0,Job.Status.WAITING,Job.JobType.POSTPROCESSING)
    #job_post.CreateParametersList(parameters)
    job_clean = Job(job_rootname+'clean',0,Job.Status.WAITING,Job.JobType.CLEANING)
    #job_clean.CreateParametersList(parameters)
    #set depency of postprocessing jobs
    job_post.setParents([job_sim.name])
    job_post.setChildren([job_clean.name])
    #set Parents of clean job
    job_clean.setParents([job_post.name])
    #set first child of sim job
    job_sim.setChildren([job_post])
    ##Set status of first chunk to READY
    if (chk==1):
     job_sim.setStatus(Job.Status.READY)
     job_sim.setParents([])
    if(chk>1):
     parentname="job_"+str(dates)+'_'+str(mem)+'_'+str(chk-1)+'_'+'sim'
     job_sim.setParents([parentname])
     if (chk>2):
      parentname="job_"+str(dates)+'_'+str(mem)+'_'+str(chk-2)+'_'+'clean'
      job_sim.addParents(parentname)
    if (chk<num_chunks):
     childname="job_"+str(dates)+'_'+str(mem)+'_'+str(chk+1)+'_'+'sim'
     job_sim.addChildren(childname)
    if (chk<num_chunks-1):
     childname="job_"+str(dates)+'_'+str(mem)+'_'+str(chk+2)+'_'+'sim'
     job_clean.setChildren([childname])

    printJobs([job_sim ,job_post ,job_clean])
    joblist+=[job_sim ,job_post ,job_clean]
  
 updateGenealogy(joblist)
 return joblist   
    
def CreateJobList2():
 joblist=list()
 #param=dict()
 proc_list=([1,1],[4,4],[2,2],[4,2],[2,4],[2,1],[1,2],[4,1],[1,4])
 for proc in proc_list:
  print "PROC %s %s" %(proc[0],proc[1])
  job_rootname="nemo-comp_"+str(proc[0])+'_'+str(proc[1])
  job_comp = Job(job_rootname,0,Job.Status.READY,Job.JobType.SIMULATION)
  #no parents nor children
  job_comp.setParents([])
  job_comp.setChildren([])
  printJobs([job_comp])
  joblist+=[job_comp]

 updateGenealogy(joblist)
 return joblist   
    

if __name__ == "__main__":
 #jobs = main()
 joblist1=range(1960,1975,5)
 ##jobs=CreateJobList(joblist1,1,2)
 jobs=CreateJobList2()
 printJobs(jobs)
 ##updateGenealogy(jobs)
 print '\nSorting by Name'.upper()
 sortJobs = sortByName(jobs)
 printJobs(sortJobs)
 file1=open('../auxfiles/joblistbyname.pkl','w')
 pickle.dump(sortJobs, file1)
 file1.close()
 #print '\nSorting by Id'.upper()
 #sortJobs = sortById(jobs)
 #printJobs(sortJobs)
 #print '\nSorting by Status'.upper()
 #sortJobs = sortByStatus(jobs)
 #printJobs(sortJobs)
 #updateJobList(jobs)
 #print '\nSorting by Status'.upper()
 #sortJobs = sortByStatus(jobs)
 #printJobs(sortJobs)
 jobs[0].setStatus(Job.Status.COMPLETED)
 updateJobList(jobs)
 print '\nSorting by Status'.upper()
 sortJobs = sortByStatus(jobs)
 file2=open('../auxfiles/joblistbystatus.pkl','w')
 pickle.dump(sortJobs, file2)
 file2.close()
 picjoblist=pickle.load(file('../auxfiles/joblistbystatus.pkl','r'))
 printJobs(picjoblist)

