#!/usr/bin/env python
from optparse import OptionParser
from ConfigParser import SafeConfigParser
import time, os, sys
import commands
import newparse_mnq as parse_mnq
import JobListFactory
import signal
import newparseMnqXml as mnq
import userdefinedfunctions
import random
import logging
import cfuConfigParser
import ItQueue
import MnQueue
import Exper

####################
# Global Variables
####################

jobList = list()
SLEEPING_TIME = 10
SUCCESS = 0
FAILURE = 1

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(name)s %(levelname)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    filename='../tmp/myauyto.log',
                    filemode='w')
logger = logging.getLogger("AutoLog")
# Hash with number of jobs in queues and running
queueStatus = dict()
# adding a new comment for testing

##TODO add an option to pass the expid on the command line.
## TODO pass jobtemplate as an optional argument to CreateJobScript

def log_long(message):
 print "[%s] %s" % (time.asctime(),message)
 
def log_short(message):
 d = time.localtime()
 date = "%04d-%02d-%02d %02d:%02d:%02d" % (d[0],d[1],d[2],d[3],d[4],d[5])
 print "[%s] %s" % (date,message)

def getActiveJobs(queueStatus):
 return queueStatus.get('active')

def getWaitingJobs(queueStatus):
 return int(queueStatus.get('eligible'))+int(queueStatus.get('blocked'))

def goToSleep(value):
 logger.info("Going to sleep (%s seconds) before retry..." % value)
 time.sleep(value)

def submitJob(scriptName, queue, debug=0):
 jobid=0
 if not debug:
  jobid=queue.submit_job(scriptName)
 else:
  os.system('bash %s' % scriptName)
  jobid=random.randrange(0,1000000)
 return jobid


####################
# Main Program
####################
if __name__ == "__main__":
 
 os.system('clear')
 parser = SafeConfigParser()
 if(len(sys.argv) != 2):
  print "Error missing config file"
 else:
  parser=cfuConfigParser.cfuConfigParser(sys.argv[1])
 
 alreadySubmitted=parser.get('config','alreadysubmitted')
 totalJobs=parser.get('config','totaljobs')
 myTemplate=parser.get('config','jobtemplate')
 expid=parser.get('config','expid')
 maxWaitingJobs=int(parser.get('config','maxwaitingjobs'))
 safetysleeptime=int(parser.get('config','safetysleeptime'))
 if(parser.get('config', hpc) == "marenostrum"):
	 queue = MnQueue(expid)
 elif(parser.get('config', hpc) == "ithaca"):
 	queue = ItQueue(expid)
 logger.debug("My template name is: %s" % myTemplate)
 logger.debug("The Experiment name is: %s" % expid)
 logger.info("Jobs to submit: %s" % totalJobs)
 logger.info("Start with job number: %s" % alreadySubmitted)
 logger.info("Maximum waiting jobs in queues: %s" % maxWaitingJobs)
 logger.info("Sleep: %s" % safetysleeptime)
 logger.info("Starting job submission...")

 if parser.get('config','restart').lower()=='true':
  filename='../auxfiles/joblist_'+expid+'.pkl'
  if (os.path.exists(filename)):
   joblist= JobListFactory.loadJobList(filename)
   logger.info("Restarting from joblist pickled in %s " % filename)
  else:
   logger.error("The pickle file %s necessary for restart is not present!" % filename)
   sys.exit()
 else: 
  #get the experiment from the database!
  #exper=Exper.getfromdatabase(expid)
  #joblist=.Exper.getJoblist()
  ##Creating the Exper object from scratch
  exper=Exper(expid,1) 
  exp_parser_name='../auxfiles/expdef_'+expid+'.file'
  expparser=cfuConfigParser.experConfigParser(exp_parser_name)
  exper.setParser(expparser)
  exper.setup()
  joblist=userdefinedfunctions.CreateJobList(expid)
 
 newlistname='../auxfiles/joblist_'+expid+'2Bupdated.pkl'
 #joblist=JobListFactory.CreateJobList2()
 logger.debug("Length of joblist: ",len(joblist))
 totaljobs=len(joblist)
 logger.info("New Jobs to submit: "+str(totaljobs)) 
 # Main loop. Finishing when all jobs have been submitted
 while JobListFactory.getNotInQueue(joblist).__len__()!=0 :
  #queueStatus=parse_mnq.updateQueueStatus(queueStatus)
  #waiting = getWaitingJobs(queueStatus)
  #active = getActiveJobs(queueStatus)
  active = JobListFactory.getRunning(joblist)
  waiting = JobListFactory.getSubmitted(joblist) + JobListFactory.getQueuing(joblist)
  available = maxWaitingJobs-waiting
  if (os.path.exists(newlistname)):
   d = time.localtime()
   date = "%04d-%02d-%02d_%02d:%02d:%02d" % (d[0],d[1],d[2],d[3],d[4],d[5])
   newlist=JobListFactory.loadJobList(newlistname)
   joblist+=newlist
   os.system('mv %s %s' % (newlistname,newlistname+'_'+date))

  logger.info("saving joblist")
  JobListFactory.saveJobList(joblist,'../auxfiles/joblist.pkl')
  graphname=joblist[0].getExpid()+'_graph.png'
  if  (os.path.exists(graphname)):
   pathname='/gpfs/projects/ecm86/common/db'
   if  (os.path.exists(pathname)):
    os.system('cp %s %s' % (graphname,pathname))
  if parser.get('congig','verbose').lower()=='true':
   logger.info("Active jobs in queues:\t%s" % active)
   logger.info("Waiting jobs in queues:\t%s" % waiting)

  if available == 0:
   if  parser.get('congig','verbose').lower()=='true':
    logger.info("There's no room for more jobs...")
  else:
   if  parser.get('congig','verbose').lower()=='true':
    logger.info("We can safely submit %s jobs..." % available)
  
  #get the list of jobs currently in the Queue
  jobinqueue=JobListFactory.getInQueue(joblist)
  logger.info("number of jobs in queue :%s" % jobinqueue.__len__()) 
  #JobListFactory.checkjobInList(jobinqueue)
  for job in jobinqueue:
   job.print_job()
   status=queue.check_job(job.get_id())
   if(status==5):
    joblist_logger.debug("this job seems to have completed...checking")
    job.check_completion()
    job.remove_dependencies()
   else:
    job.set_status(status) 
   
  ##after checking the jobs , no job should have the status "submitted"
  ##Jordi throw an exception if this happens (warning type no exit)
  if (JobListFactory.getReady(joblist).__len__()!=0) :
   JobListFactory.printJobs(JobListFactory.getReady(joblist))
   
  #update joblist
  #we only need to update the active jobs
  JobListFactory.updateJobList(joblist)
  activejobs=JobListFactory.getActive(joblist)
  logger.info("in the factory there is %s active jobs" % activejobs.__len__())

  ## get the list of jobs READY
  jobsavail=JobListFactory.getReady(joblist)
  if (min(available,len(jobsavail)) ==0):
   logger.info("There is no job READY or available")
   logger.info("Number of job ready: ",len(jobsavail))
   logger.info("Number of jobs available in queue:", available)
  elif (min(available,len(jobsavail)) > 0): 
   logger.info("We are gonna submit: ", min(available,len(jobsavail)))
   ##should sort the jobsavail by priority Clean->post->sim>ini
   JobListFactory.printJobs(jobsavail)
   jobsavail=JobListFactory.sortByType(jobsavail)
   
   jobsavail.reverse()
   for job in jobsavail[0:min(available,len(jobsavail))]:
    scriptname=userdefinedfunctions.CreateJobScript(job) 
    print scriptname
    jobid=submitJob(scriptname, queue)
    job.setId(jobid)
    ##set status to "submitted"
    job.setStatus(2)
    if  parser.get('congig','clean').lower()=='true':
     os.system("rm %s" % scriptname)

    alreadySubmitted += 1
    
   logger.info("We have already submitted %s of %s jobs" % (alreadySubmitted,totaljobs))
  
  if JobListFactory.getActive(joblist).__len__()!=0:
   goToSleep(safetysleeptime)
 
 logger.info("Finished job submission")
 JobListFactory.updateJobList(joblist)
 JobListFactory.printJobs(joblist)
 if  parser.get('congig','verbose').lower()=='true':
  #queueStatus=parse_mnq.updateQueueStatus(queueStatus)
  #waiting = getWaitingJobs(queueStatus)
  #active = getActiveJobs(queueStatus)
  active = JobListFactory.getRunning(joblist)
  waiting = JobListFactory.getSubmitted(joblist) + JobListFactory.getQueuing(joblist)
  logger.info("Active jobs in queues:\t%s" % active)
  logger.info("Waiting jobs in queues:\t%s" % waiting)

