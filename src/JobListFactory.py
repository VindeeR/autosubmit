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
import sys
sys.path.append('job')
from job_common import Status
from job_common import Type


joblist_logger = logging.getLogger("AutoLog.JobList")
         
def compareStatus(job_a,job_b):
 return cmp(int(job_a.get_status()),int(job_b.get_status()))

def compareType(job_a,job_b):
 return cmp(int(job_a.get_job_type()),int(job_b.getJobType()))


def compareId(job_a,job_b):
 return cmp(int(job_a.get_id()),int(job_b.get_id()))

def compareName(job_a,job_b):
 return cmp(job_a.get_name(),job_b.get_name())

def sortByName(jobs):
 return sorted(jobs,compareName)

def get_name(jobs,name):
 for job in jobs:
  if(job.get_name()==name):
   return job

def checkjobInList(jobs):
 for job in jobs:
  job.print_job()
  status=parse_mnq.checkjob(job.get_id())
  if(status==5):
   joblist_logger.debug("this job seems to have completed...checking")
   job.check_completion()
  else:
   job.set_status(status) 

def loadJobList(newfilename):
 joblist_logger.info("Loading joblist  %s" % newfilename)
 jobs=pickle.load(file(newfilename,'r'))
 return jobs

def saveJobList(jobs,filename):
 expid=jobs[0].get_expid()
 newfilename=filename.split('.pkl')[0]
 newfilename+='_'+expid+'.pkl'
 joblist_logger.info("Saving joblist into %s" % newfilename)
 pickle.dump(jobs,file(newfilename,'w'))
 #monitor.CreateTreeList(jobs)

def printJobs(jobs):
 joblist_logger.info("%s\t%s\t%s" % ("Job Name","Job Id",""))
 for job in jobs:
  joblist_logger.info("%s\t\t%s\t%s" % (job.get_name(),job.get_id(),job.get_status()))

def getCompleted(jobs):
 jobcompleted=[job for job in jobs if job.get_status()==5]
 return jobcompleted

def getSubmitted(jobs):
 jobsubmitted=[job for job in jobs if job.get_status()==2]
 return jobsubmitted

def getRunning(jobs):
 jobl=[job for job in jobs if job.get_status()==4]
 return jobl

def getQueuing(jobs):
 jobl=[job for job in jobs if job.get_status()==3]
 return jobl

def getFailed(jobs):
 jobl=[job for job in jobs if job.get_status()==-1]
 return jobl
 
def getReady(jobs):
 jobl=[job for job in jobs if job.get_status()==1]
 return jobl

def getWaiting(jobs):
 jobl=[job for job in jobs if job.get_status()==0]
 return jobl

def getInQueue(jobs):
 jobl=[job for job in jobs if job.get_status()>1 and job.get_status()<5]
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

def remove_duplicate(jobs):
 namejobs=[]
 newjobs=[]
 for j in jobs:
  jname=j.getName()
  if not jname in namejobs:
   namejobs+=[jname]
   newjobs+=[j]
 return newjobs  

def getListOfNames(jobs):
 listofnames=[]
 for j in jobs:
  listofnames+=[j.getName()]
 return listofnames 

def updateJobList(jobs,save=1):
 joblist_logger.info("*******************UPDATING THE LIST****************************")
 failed=[]
 filename='../auxfiles/failed_joblist.pkl'
 expid=jobs[0].get_expid()
 newlistname=filename.split('.pkl')[0]
 newlistname+='_'+expid+'.pkl'
 if (os.path.exists(newlistname)):
  failed=loadJobList(newlistname)
  if save:
   os.system('rm %s' % newlistname)
   print "removing %s" % newlistname
   for j in failed:
    if j.getName() in namelist:
     joblist_logger.info("Job %s is present in the joblist AND in the failed one" % j.getName())
     failed.remove(j)

 for job in jobs:
  if (job.get_status()==-1):
   count=job.get_fail_count()
   job.printJob()
   job.set_fail_count(count+1)
   if (job.get_fail_count()<4):
    job.set_status(Status.READY)
   elif job.get_fail_count()==4:
    joblist_logger.info("Job %s has failed 4 times" % job.get_name())
    children=job.get_all_children()
    failed+=[job]
    jobs.remove(job)
    joblist_logger.info(" Now failing all of its heirs...")
    #printJobs(children)
    for child in children:
     child.set_status(Status.FAILED)
     child.set_fail_count(5)
     failed+=[child]
     if jobs.__contains__(child):
      jobs.remove(child)
   elif job.get_fail_count()>=5:
     joblist_logger.debug("Job %s has already been canceled!!!!" % job.get_name())
  elif job.get_status()==0 and job.has_parents()==0:
   joblist_logger.info("job is now set to be ready: %s" % job.get_name())
   job.set_status(Status.READY)
 
 if failed.__len__()>0:
  if save:
   saveJobList(failed,filename)
 if save:
  saveJobList(jobs,'../auxfiles/joblist.pkl') 


def updateGenealogy(jobs):
 joblist_logger.info("in genealogy!")
 for job in jobs:
  job.printJob()
  if(job.has_children()!=0):
   ##print "number of Children:",job.has_children()
   children=job.get_children()
   ##print children
   #reset job.children list
   job.set_children([])
   for child in children:
    if isinstance(child,str):
     jobchild=get_name(jobs,child)
     ##print "childname %s has type:" % child, type(jobchild)
     job.add_children(jobchild)
    else:
     ##print "surely child has already the type job:",child
     child.printJob()
     job.add_children(child)
   
  if(job.has_parents()!=0):
   ##print "Number of Parents:",job.has_parents()
   parents=job.get_parents()
   ##print parents
   #reset job.children list
   job.set_parents([])
   for parent in parents:
    if isinstance(parent,str):
     ##print "parentname %s has type:" % parent, type(parent)
     jobparent=get_name(jobs,parent)
     job.add_parents(jobparent)
    else:
     ##print "surely parent has already a type job",parent
     #parent.printJob()
     job.add_parents(parent)
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
  joblname+=[j.get_name()]
 print len(crosslist)
 specialp=get_name(jobl,'job_19801101_3_10_post')
 specialc=get_name(jobl,'job_19801101_3_12_sim')
 if os.path.exists('../auxfiles/failed_joblist_yve2.pkl'):
  jobsfailed=pickle.load(file('../auxfiles/failed_joblist_yve2.pkl','r'))
  print 'FAILED joblist!!! ', jobsfailed.__len__()
  for job in jobsfailed:
   if job.get_name()=='job_19801101_3_10_clean':
    specialj=job
    print specialj.has_parents()
    if specialj.has_parents():
     for p in specialj.get_parents():
      print 'parent: %s' %p.get_name()
    else:
     print 'adding parent %s' %specialp.get_name()
     specialj.set_parents([specialp])
    print specialj.has_children()
    if specialj.has_children():
     for c in specialj.get_children():
      print 'child: %s' %c.get_name()
    else:
     print 'adding child: %s' % specialc.get_name()
     specialj.set_children([specialc])
 if joblname.__contains__('job_19801101_3_10_clean'):
  print 'already has the special job resetting...'
  specialj=get_name(jobl,'job_19801101_3_10_clean')
  specialj.set_status(0)
  specialj.set_parents([specialp])
  specialj.set_children([specialc])
 else:
  jobl+=[specialj]
 manual_job_list=[]
 manual_list=["job_19801101_3_3_sim","job_19851101_0_3_sim","job_19851101_2_3_sim",'job_19851101_3_3_sim','job_19901101_4_3_sim','job_19901101_2_3_sim','job_19901101_1_4_sim','job_19851101_4_3_sim']
 for job in jobl:
  if manual_list.__contains__(job.get_name()):
    manual_job_list+=[job]
    job.set_status(Status.READY)

 #listtoadd=[]
 #newlistyve2=userdefinedfunctions.CreateJobList('yve2')
 #printJobs(newlistyve2)
 #updateJobList(newlistyve2,save=0)
 #print "newlistyve2 has %s jobs" % newlistyve2.__len__()
 #for job in newlistyve2:
 # if manual_list.__contains__(job.get_name()):
 #  job.printJob()
 #  job.set_status(Status.READY)
 #  print job.get_name()
 #  print job.has_children()
 #  for child in job.get_children():
 #   print type(child)
 #   print child.get_name()
 #  toto=job.get_all_children()
 #  logger2.info('Adding new jobs')
 #  printJobs(toto)
 #  listtoadd+=[job]
 #  listtoadd+=toto
 #print "NEW JOB to ADD ",listtoadd.__len__()
# jobsfailed=[]
 #jobcrosslist=[job for job in jobl if crosslist.__contains__(job.get_name())]
 #for job in manual_job_list:
 #  job.set_status(5)
 #  print "setting complete: %s" %job.get_name()
 #  job.check_completion(touched=0) 
   #children=job.get_all_children()
   #for child in children:
   # child.set_status(0)
   # child.set_fail_count(0)
 #jobcrosslist2=[]
 #jobcrosslist2=[job for job in jobsfailed if not jobs.__contains__(job)]

 
 updateJobList(jobl,save=0)
 #jobs+= listtoadd  
 failed=getFailed(jobl)
 #printJobs(failed)
 for j in failed:
  print 'FAILED: %s' %j.get_name()
  if j.get_name()=='job_19801101_3_10_clean':
   print "What the hell is going on here?"
  else:
   j.set_status(1)
   j.set_fail_count(0)
 print 'FAILED!!! ', failed.__len__()
 inqueue=getInQueue(jobl)
 #for job in inqueue:
 # job.set_status(1)
 
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
#   if job.get_status() < 5:
#    job.set_status(job.get_status()+1)
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
