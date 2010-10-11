#!/usr/bin/env python

import logging
import pickle
import newparse_mnq as parse_mnq
from Job import *
import userdefinedfunctions
import time, os
#import monitor
import commands



joblist_logger = logging.getLogger("AutoLog.JobList")
         
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
   joblist_logger.debug("this job seems to have completed...checking")
   job.check_completion()
  else:
   job.setStatus(status) 

def loadJobList(newfilename):
 joblist_logger.info("Loading joblist  %s" % newfilename)
 jobs=pickle.load(file(newfilename,'r'))
 return jobs

def saveJobList(jobs,filename):
 expid=jobs[0].getExpid()
 newfilename=filename.split('.pkl')[0]
 newfilename+='_'+expid+'.pkl'
 joblist_logger.info("Saving joblist into %s" % newfilename)
 pickle.dump(jobs,file(newfilename,'w'))
 #monitor.CreateTreeList(jobs)

def cancelJobList(jobs):
 for job in jobs:
  parse_mnq.cancelJob(job.getId()) 
def printJobs(jobs):
 joblist_logger.info("%s\t%s\t%s" % ("Job Name","Job Id","Job Status"))
 for job in jobs:
  joblist_logger.info("%s\t\t%s\t%s" % (job.getName(),job.getId(),job.getStatus()))

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
 joblist_logger.info("*******************UPDATING THE LIST****************************")
 failed=[]
 filename='../auxfiles/failed_joblist.pkl'
 expid=jobs[0].getExpid()
 newlistname=filename.split('.pkl')[0]
 newlistname+='_'+expid+'.pkl'
 if (os.path.exists(newlistname)):
  failed=loadJobList(newlistname)
  os.system('rm %s' % newlistname)
  
 for job in jobs:
  if (job.getStatus()==-1):
   count=job.getFailCount()
   job.printJob()
   job.setFailCount(count+1)
   if (job.getFailCount()<4):
    job.setStatus(Job.Status.READY)
   elif job.getFailCount()==4:
    joblist_logger.info("Job %s has failed 4 times" % job.getName())
    children=job.getAllChildren()
    failed+=[job]
    jobs.remove(job)
    joblist_logger.info(" Now failing all of its heirs...")
    #printJobs(children)
    for child in children:
     child.setStatus(Job.Status.FAILED)
     child.setFailCount(5)
     failed+=[child]
     if jobs.__contains__(child):
      jobs.remove(child)
   elif job.getFailCount()>=5:
     joblist_logger.debug("Job %s has already been canceled!!!!" % job.getName())
  elif job.getStatus()==0 and job.hasParents()==0:
   joblist_logger.info("job is now set to be ready: %s" % job.getName())
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
 joblist_logger.info("in genealogy!")
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
 joblist_logger.debug("after genealogy!")


if __name__ == "__main__":
 logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(name)s %(levelname)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    filename='myapp.log',
                    filemode='w')
 logger2 = logging.getLogger("JobListLog")
 logger2.debug('A debug message')
 logger2.info('Some information')
 logger2.warning('A shot across the bows')
 manual_list=[]
 checklist=[]
 filelist=commands.getoutput('ls mylogs |grep COMPLETED').split()
 filechecked=commands.getoutput('ls *Checked').split()
 jobs=pickle.load(file('../auxfiles/joblist.pkl','r'))
 
 for job in jobs:
   logger2.info('Job %s has status: %s' % (job.getName(),job.getStatus()))
 print "finished"       
               
 