#!/usr/bin/env python
from optparse import OptionParser
from ConfigParser import SafeConfigParser
import time, os, sys
import commands
import newparse_mnq as parse_mnq
import JobListFactory
import signal
import newparseMnqXml as mnq
#import userdefinedfunctions
import random
import logging
import cfuConfigParser
from queue.itqueue import ItQueue
from queue.mnqueue import MnQueue
from Exper import Exper
from job.job import Job
from job.job_common import Status
from job.job_list import JobList
import cPickle as pickle
import chunk_date_lib

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
                    filename='../tmp/myauto.log',
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
 
 alreadySubmitted=int(parser.get('config','alreadysubmitted'))
 totalJobs=int(parser.get('config','totaljobs'))
 myTemplate=parser.get('config','jobtemplate')
 expid=parser.get('config','expid')
 maxWaitingJobs=int(parser.get('config','maxwaitingjobs'))
 safetysleeptime=int(parser.get('config','safetysleeptime'))
 if(parser.get('config', 'hpcarch') == "marenostrum"):
	 queue = MnQueue(expid)
 elif(parser.get('config', 'hpcarch') == "ithaca"):
 	queue = ItQueue(expid)
 logger.debug("My template name is: %s" % myTemplate)
 logger.debug("The Experiment name is: %s" % expid)
 logger.info("Jobs to submit: %s" % totalJobs)
 logger.info("Start with job number: %s" % alreadySubmitted)
 logger.info("Maximum waiting jobs in queues: %s" % maxWaitingJobs)
 logger.info("Sleep: %s" % safetysleeptime)
 logger.info("Starting job submission...")

 if parser.get('config','restart').lower()=='true':
  filename='../auxfiles/job_list_'+expid+'.pkl'
  if (os.path.exists(filename)):
   joblist= pickle.load(file(filename,'rw'))
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
  exp_parser_name=parser.get('config','EXPDEFFILE')
  expparser=cfuConfigParser.experConfigParser(exp_parser_name)
  exper.setParser(expparser)
  exper.setup()
  listofdates=expparser.get('expdef','DATELIST')
  chunkini=int(expparser.get('expdef','CHUNKINI'))
  numchunks=int(expparser.get('expdef','NUMCHUNKS'))
  memberslist=expparser.get('expdef','MEMBERS')
  
  print listofdates
  print chunkini
  print numchunks
  print memberslist
  
  for d in listofdates:
   print d
  for m in memberslist: 
   print m
  stop  
  joblist=JobList(expid,listofdates,memberslist,chunkini,numchunks)
  queue.check_pathdir()
  
 
 logger.debug("Length of joblist: ",len(joblist))
 totaljobs=len(joblist)
 logger.info("New Jobs to submit: "+str(totaljobs))
 list_of_common_para=expparser.items('common_parameters')
 parameters=dict()
 for it in list_of_common_para:
  parameters[it[0].upper()]= it[1]
 parameters['SHELL'] = "/bin/ksh"
 parameters['Chunk_NUMBERS']='15'
 parameters['Chunk_SIZE_MONTH']='4'
 parameters['INITIALDIR']='/home/ecm86/ecm86503/LOG_'+expid
 parameters['LOGDIR']='/home/ecm86/ecm86503/LOG_'+expid
 parameters['EXPID']=expid
 parameters['VERSION']='v2.2.1'
 for j in joblist.get_job_list():
  j.set_parameters(parameters) 
 template_rootname=expparser.get('common_parameters','TEMPLATE') 
 # Main loop. Finishing when all jobs have been submitted
 while len(joblist.get_not_in_queue())!=0 :
  #queueStatus=parse_mnq.updateQueueStatus(queueStatus)
  #waiting = getWaitingJobs(queueStatus)
  #active = getActiveJobs(queueStatus)
  active = len(joblist.get_running())
  waiting = len(joblist.get_submitted() + joblist.get_queuing())
  available = maxWaitingJobs-waiting
  
  logger.info("saving joblist")
  joblist.save()
  graphname=exper.getExpid()+'_graph.png'
  if  (os.path.exists(graphname)):
   pathname='/gpfs/projects/ecm86/common/db'
   if  (os.path.exists(pathname)):
    os.system('cp %s %s' % (graphname,pathname))
  if parser.get('config','verbose').lower()=='true':
   logger.info("Active jobs in queues:\t%s" % active)
   logger.info("Waiting jobs in queues:\t%s" % waiting)

  if available == 0:
   if  parser.get('config','verbose').lower()=='true':
    logger.info("There's no room for more jobs...")
  else:
   if  parser.get('config','verbose').lower()=='true':
    logger.info("We can safely submit %s jobs..." % available)
  
  #get the list of jobs currently in the Queue
  jobinqueue=joblist.get_in_queue()
  logger.info("number of jobs in queue :%s" % len(jobinqueue)) 
  #JobListFactory.checkjobInList(jobinqueue)
  for job in jobinqueue:
   job.print_job()
   status=queue.check_job(job.get_id())
   if(status==5):
    logger.debug("this job seems to have completed...checking")
    job.check_completion()
    job.remove_dependencies()
   else:
    job.set_status(status) 
   
  ##after checking the jobs , no job should have the status "submitted"
  ##Jordi throw an exception if this happens (warning type no exit)
  for job in joblist.get_ready():
   job.print_job()
   
  joblist.update_list()
  activejobs=joblist.get_active()
  logger.info("in the factory there is %s active jobs" % len(activejobs))

  ## get the list of jobs READY
  jobsavail=joblist.get_ready()
  if (min(available,len(jobsavail)) ==0):
   logger.info("There is no job READY or available")
   logger.info("Number of job ready: ",len(jobsavail))
   logger.info("Number of jobs available in queue:", available)
  elif (min(available,len(jobsavail)) > 0): 
   logger.info("We are gonna submit: ", min(available,len(jobsavail)))
   ##should sort the jobsavail by priority Clean->post->sim>ini
   list_of_jobs_avail=sorted(jobsavail, key=lambda k:k.get_type())
     
   for job in list_of_jobs_avail[0:min(available,len(jobsavail))]:
    print job.get_name()
    scriptname=job.create_script(template_rootname) 
    print scriptname
    queue.send_script(scriptname)
    jobid=submitJob(scriptname, queue)
    job.set_id(jobid)
    ##set status to "submitted"
    job.set_status(2)
    if  parser.get('config','clean').lower()=='true':
     os.system("rm %s" % scriptname)

    alreadySubmitted += 1
    
   logger.info("We have already submitted %s of %s jobs" % (alreadySubmitted,totaljobs))
  
  if len(joblist.get_active())!=0:
   goToSleep(safetysleeptime)
 
 logger.info("Finished job submission")
 joblist.update_list()
 
 if  parser.get('config','verbose').lower()=='true':
  active = len(joblist.get_running())
  waiting = len(joblist.get_submitted() + joblist.get_queuing())
  logger.info("Active jobs in queues:\t%s" % active)
  logger.info("Waiting jobs in queues:\t%s" % waiting)

