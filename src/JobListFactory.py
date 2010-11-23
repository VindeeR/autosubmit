#!/usr/bin/env python

import logging
import pickle
import newparse_mnq as parse_mnq
from Job import *
import userdefinedfunctions
import time, os
#import monitor
import commands
import types


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

def updateJobList(jobs,save=1):
 joblist_logger.info("*******************UPDATING THE LIST****************************")
 failed=[]
 filename='../auxfiles/failed_joblist.pkl'
 expid=jobs[0].getExpid()
 newlistname=filename.split('.pkl')[0]
 newlistname+='_'+expid+'.pkl'
 if (os.path.exists(newlistname)):
  failed=loadJobList(newlistname)
  if save:
   os.system('rm %s' % newlistname)
   print "removing %s" % newlistname

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
  if save:
   saveJobList(failed,filename)
 if save:
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
     print "childname %s has type:" % child, type(jobchild)
     job.addChildren(jobchild)
    else:
     print "surely child has already the type job:",child
     #child.printJob()
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
     #parent.printJob()
     job.addParents(parent)
 return jobs
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
 parse_mnq.collect()
 checklist=[]
 filelist=commands.getoutput('ls ../tmp |grep COMPLETED').split()
 filechecked=commands.getoutput('ls ../tmp |grep Checked').split()
 for name in filechecked:
  checklist+=[name.split('Check')[0]]

 crosslist=[]
 for name in filelist:
  shortname=name.split('_COMPLETE')[0]
  #if not (checklist.__contains__(shortname)):
  crosslist+=[shortname]
 #('job_19701101_1_4_sim','job_19701101_2_4_sim','job_19701101_2_1_clean','job_19701101_0_3_clean','job_19701101_1_3_clean','job_19651101_3_3_clean')
 jobl=pickle.load(file('../auxfiles/joblist_yve2.pkl','r'))
 joblname=[]
 for j in jobl:
  joblname+=[j.getName()]
 print len(crosslist)
 specialp=getName(jobl,'job_19801101_3_10_post')
 specialc=getName(jobl,'job_19801101_3_12_sim')
 if os.path.exists('../auxfiles/failed_joblist_yve2.pkl'):
  jobsfailed=pickle.load(file('../auxfiles/failed_joblist_yve2.pkl','r'))
  print 'FAILED joblist!!! ', jobsfailed.__len__()
  for job in jobsfailed:
   if job.getName()=='job_19801101_3_10_clean':
    specialj=job
    print specialj.hasParents()
    if specialj.hasParents():
     for p in specialj.getParents():
      print 'parent: %s' %p.getName()
    else:
     print 'adding parent %s' %specialp.getName()
     specialj.setParents([specialp])
    print specialj.haschildren()
    if specialj.hasChildren():
     for c in specialj.getChildren():
      print 'child: %s' %c.getName()
    else:
     print 'adding child: %s' % specialc.getName()
     specialj.setChildren([specialc])
 if joblname.__contains__('job_19801101_3_10_clean'):
  print 'already has the special job resetting...'
  specialj=getName(jobl,'job_19801101_3_10_clean')
  specialj.setStatus(0)
  specialj.setParents([specialp])
  specialj.setChildren([specialc])
 else:
  jobl+=[specialj]
 manual_job_list=[]
 manual_list=["job_19801101_3_3_sim","job_19851101_0_3_sim","job_19851101_2_3_sim",'job_19851101_3_3_sim','job_19901101_4_3_sim','job_19901101_2_3_sim','job_19901101_1_4_sim','job_19851101_4_3_sim']
 for job in jobl:
  if manual_list.__contains__(job.getName()):
    manual_job_list+=[job]
    job.setStatus(Job.Status.READY)

 #listtoadd=[]
 #newlistyve2=userdefinedfunctions.CreateJobList('yve2')
 #printJobs(newlistyve2)
 #updateJobList(newlistyve2,save=0)
 #print "newlistyve2 has %s jobs" % newlistyve2.__len__()
 #for job in newlistyve2:
 # if manual_list.__contains__(job.getName()):
 #  job.printJob()
 #  job.setStatus(Job.Status.READY)
 #  print job.getName()
 #  print job.hasChildren()
 #  for child in job.getChildren():
 #   print type(child)
 #   print child.getName()
 #  toto=job.getAllChildren()
 #  logger2.info('Adding new jobs')
 #  printJobs(toto)
 #  listtoadd+=[job]
 #  listtoadd+=toto
 #print "NEW JOB to ADD ",listtoadd.__len__()
# jobsfailed=[]
 #jobcrosslist=[job for job in jobl if crosslist.__contains__(job.getName())]
 #for job in manual_job_list:
 #  job.setStatus(5)
 #  print "setting complete: %s" %job.getName()
 #  job.check_completion(touched=0) 
   #children=job.getAllChildren()
   #for child in children:
   # child.setStatus(0)
   # child.setFailCount(0)
 #jobcrosslist2=[]
 #jobcrosslist2=[job for job in jobsfailed if not jobs.__contains__(job)]

 
 updateJobList(jobl,save=0)
 #jobs+= listtoadd  
 failed=getFailed(jobl)
 #printJobs(failed)
 for j in failed:
  print 'FAILED: %s' %j.getName()
  if j.getName()=='job_19801101_3_10_clean':
   print "What the hell is going on here?"
  else:
   j.setStatus(1)
   j.setFailCount(0)
 print 'FAILED!!! ', failed.__len__()
 inqueue=getInQueue(jobl)
 #for job in inqueue:
 # job.setStatus(1)
 
 completed=getCompleted(jobl)
 
 print "COMPLETED!!!!",completed.__len__()
 #for job in completed:
 # job.check_completion(touched=0) 
 
 printJobs(sortByType(completed))
 ready=getReady(jobl)
 print 'READY ', ready.__len__()
 print 'FAILED!!!'
 failed=getFailed(jobl)
 printJobs(failed)

 #updateJobList(jobs)
 #sortJobs = sortByStatus(jobs)
 #printJobs(sortJobs)

 ready=getReady(jobl)
 print 'READY', ready.__len__()
 #printJobs(ready)
 completed=getCompleted(jobl)
 print "COMPLETED!!!!",completed.__len__()
 #printJobs(sortByType(completed))
 running=getRunning(jobl)
 print "RUNNING!!",running.__len__()
 printJobs(running)
 waiting=getWaiting(jobl)
 print "WAITING!!",waiting.__len__()
 queuing=getQueuing(jobl)
 print "queuing!!", queuing.__len__()
 printJobs(queuing)
 submitted=getSubmitted(jobl)
 print "submitted",submitted.__len__()
 
 inqueue=getInQueue(jobl)
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
 file2='../auxfiles/newjoblist.pkl'
 #removing duplicate:
 #jobs_bis=[]
 #for job in jobs:
 # if job not in jobs_bis:
 #  jobs_bis.append(job)
   
 saveJobList(jobl, file2)
 #time.sleep(10)
 ##picjoblist=pickle.load(file('../auxfiles/joblist.pkl','r'))
 ##printJobs(picjoblist)
 print "finished"       
