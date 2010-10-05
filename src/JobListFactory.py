#!/usr/bin/env python

import pickle
import newparse_mnq as parse_mnq
from Job import *
import userdefinedfunctions
import time, os
#import monitor
import commands

def compareStatus(job_a,job_b):
 return cmp(int(job_a.getStatus()),int(job_b.getStatus()))

def compareType(job_a,job_b):
 return cmp(int(job_a.getJobType()),int(job_b.getJobType()))


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

def checkjobInList(jobs):
 for job in jobs:
  job.printJob()
  status=parse_mnq.checkjob(job.getId())
  if(status==5):
   print "this job seems to have completed...checking"
   job.check_completion()
  else:
   job.setStatus(status) 

def loadJobList(newfilename):
 print "Loading joblist  %s" % newfilename
 jobs=pickle.load(file(newfilename,'r'))
 return jobs

def saveJobList(jobs,filename):
 expid=jobs[0].getExpid()
 newfilename=filename.split('.pkl')[0]
 newfilename+='_'+expid+'.pkl'
 print "Saving joblist into %s" % newfilename
 pickle.dump(jobs,file(newfilename,'w'))
 #monitor.CreateTreeList(jobs)

def cancelJobList(jobs):
 for job in jobs:
  parse_mnq.cancelJob(job.getId()) 
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
 jobl=[job for job in jobs if job.getStatus()>1 and job.getStatus()<5]
 return jobl

def getFinished(jobs):
 jobl=getCompleted(jobs)+getFailed(jobs)
 return jobl

def getActive(jobs):
 jobl=getInQueue(jobs)+getReady(jobs)
 return jobl

def getNotInQueue(jobs):
 jobl=getReady(jobs)+getWaiting(jobs)
 return jobl

def sortById(jobs):
 return sorted(jobs,compareId)

def sortByStatus(jobs):
 return sorted(jobs,compareStatus)

def sortByType(jobs):
 return sorted(jobs,compareType)

def updateJobList(jobs):
 print "*******************UPDATING THE LIST****************************"
 failed=[]
 filename='../auxfiles/failed_joblist.pkl'
 expid=jobs[0].getExpid()
 newlistname=filename.split('.pkl')[0]
 newlistname+='_'+expid+'.pkl'
 if (os.path.exists(newlistname)):
  failed=loadJobList(newlistname)
 
 for job in jobs:
  if (job.getStatus()==-1):
   count=job.getFailCount()
   job.printJob()
   job.setFailCount(count+1)
   if (job.getFailCount()<4):
    job.setStatus(Job.Status.READY)
   elif job.getFailCount()==4:
    print "Job %s has failed 4 times" % job.getName()
    children=job.getAllChildren()
    failed+=[job]
    jobs.remove(job)
    print " Now failing all of its heirs..."
    #printJobs(children)
    for child in children:
     child.setStatus(Job.Status.FAILED)
     child.setFailCount(5)
     failed+=[child]
     if jobs.__contains__(chid):
      jobs.remove(child)
   elif job.getFailCount()>=5:
     print "Job %s has already been canceled!!!!" % job.getName()
  elif job.getStatus()==0 and job.hasParents()==0:
   print "job is now set to be ready: %s" % job.getName()
   job.setStatus(Job.Status.READY)
 
 if failed.__len__()>0:
  saveJobList(failed,filename)
 saveJobList(jobs,'../auxfiles/joblist.pkl') 

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


if __name__ == "__main__":
 ##jobs = main()
 manual_list=[]
 checklist=[]
 filelist=commands.getoutput('ls mylogs |grep COMPLETED').split()
 filechecked=commands.getoutput('ls *Checked').split()
 for name in filelist:
  manual_list+=[name.split('_COMPLETE')[0]]
 for name in filechecked:
  checklist+=[name.split('Checke')[0]]
 crosslist=[]
 
 for name in manual_list:
  if not (checklist.__contains__(name)):
   crosslist+=[name]
 #('job_19701101_1_4_sim','job_19701101_2_4_sim','job_19701101_2_1_clean','job_19701101_0_3_clean','job_19701101_1_3_clean','job_19651101_3_3_clean')
 jobs=pickle.load(file('../auxfiles/joblist_yve2.pkl','r'))
 print crosslist
 #joblist1=range(1960,1975,5)
 #jobs=userdefinedfunctions.CreateJobList("yves")
 ##jobs=CreateJobList2()
 #printJobs(jobs)
 #otherlist=('job_19601101_4_3_post','job_19651101_3_5_post','job_19701101_0_5_post','job_19701101_1_6_post','job_19651101_4_2_sim')
 jobcrosslist=[job for job in jobs if crosslist.__contains__(job.getName())]
 for job in jobcrosslist:
   job.setStatus(5)
   print "setting complete: %s" %job.getName()
   job.check_completion() 
   children=job.getAllChildren()
   for child in children:
    child.setStatus(0)
    child.setFailCount(0)

 otherlist=[job for job in jobs if job.getName()=='job_19751101_4_1_sim'] 
 for job in otherlist:
   job.setStatus(4)
   job.setId(2905679)
 #  print "setting complete: %s" %job.getName()
 #  job.check_completion() 

# failed=getFailed(jobs)
# for job in failed:
#  if job.getId()>0:
#   job.setStatus(1)
#   job.setFailCount(0)
#  else:
#   job.setStatus(0)
#   job.setFailCount(0)
 
 # if job.getName()=='job_19701101_2_8_sim':
 #  job.setStatus(1)
 #  print "setting ready: %s" %job.getName()
 # if otherlist.__contains__(job.getName()):
 #  job.setStatus(5)
 #  print "setting complete: %s" %job.getName()
 #  job.check_completion()
 #  children=job.getAllChildren()
 #  for child in children:
 #   child.setStatus(0)
 #   child.setFailCount(0)
    
 updateJobList(jobs)
 #completed=getCompleted(jobs)
 #print "COMPLETED!!!!"
 #printJobs(completed)
 #ready=getReady(jobs)
 #print 'READY'
 #printJobs(ready)
 failed=getFailed(jobs)
 #printJobs(failed)
 print 'FAILED!!! ', failed.__len__()
 ##updateGenealogy(jobs)
 #for job in failed:
  #job.setStatus(0)
  #job.setFailCount(0)
   #job.check_completion()
  #if job.getName()=='job_19601101_1_1_sim' or job.getName()=='job_19601101_0_1_sim':
  # job.setId(0)
  # job.setStatus(1) 
 #print '\nSorting by Name'.upper()
 #for job in jobs:
 # if job.getStatus() > 1:
 #  job.check_completion()
 
 completed=getCompleted(jobs)
 print "COMPLETED!!!!",completed.__len__()
 printJobs(sortByType(completed))
 ready=getReady(jobs)
 print 'READY ', ready.__len__()
## printJobs(ready)
 print 'FAILED!!!'
 failed=getFailed(jobs)
 printJobs(failed)

 #updateJobList(jobs)
 #sortJobs = sortByStatus(jobs)
 #printJobs(sortJobs)

 ready=getReady(jobs)
 print 'READY', ready.__len__()
 #printJobs(ready)
 completed=getCompleted(jobs)
 print "COMPLETED!!!!",completed.__len__()
 #printJobs(sortByType(completed))
 running=getRunning(jobs)
 print "RUNNING!!",running.__len__()
 printJobs(running)
 waiting=getWaiting(jobs)
 print "WAITING!!",waiting.__len__()
 queuing=getQueuing(jobs)
 print "queuing!!", queuing.__len__()
 printJobs(queuing)
 submitted=getSubmitted(jobs)
 print "submitted",submitted.__len__()
 
 inqueue=getInQueue(jobs)
 print "InQueue!!", inqueue.__len__()
 printJobs(inqueue)
 #monitor.CreateTreeList(jobs)
 #print crosslist,crosslist.__len__()
 #print checklist,checklist.__len__()
 #print manual_list, manual_list.__len__() 
 #file1='../auxfiles/joblistbyname.pkl'
 #saveJobList(sortJobs, file1)
 #total=jobs.__len__()
 #finished=0
 #updateJobList(jobs)
# while finished!=total:
#  for job in jobs:
#   if job.getStatus() < 5:
#    job.setStatus(job.getStatus()+1)
#  updateJobList(jobs) 
#  finished=getFinished(jobs).__len__()
#  print "%s finished jobs out of %s total" % (finished,total)
#  print '\nSorting by Status'.upper()
#  sortJobs = sortByStatus(jobs)
 file2='../auxfiles/joblist.pkl'
 saveJobList(jobs, file2)
 #time.sleep(10)
 ##picjoblist=pickle.load(file('../auxfiles/joblist.pkl','r'))
 ##printJobs(picjoblist)
 print "finished"
