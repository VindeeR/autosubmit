#!/usr/bin/env python
from optparse import OptionParser
import time, os, sys
import commands
import parse_mnq
import JobListFactory
import signal
import pickle
import parseMnqXml as mnq
import userdefinedfunctions
import random
####################
# Global Variables
####################

jobList = list()
SLEEPING_TIME = 10
SUCCESS = 0
FAILURE = 1

# Hash with number of jobs in queues and running
queueStatus = dict()
# adding a new comment for testing
def handle_options():
 parser = OptionParser(usage="usage: %prog [options]",version="%prog 1.0")
 parser.add_option("-m","--maxjobs",
                   type="int",
                   dest="maxWaitingJobs",
		   default=50,
		   help="Keep always the number of jobs in queues lower than maxWaitingJobs.",
		   metavar="jobs")
 parser.add_option("-t","--totaljobs",
                   type="int",
                   dest="totalJobs",
		   default=1000,
		   help="Submit totalJobs jobs.",
		   metavar="jobs")
 parser.add_option("-n","--startingjobs",
                   type="int",
                   dest="alreadySubmitted",
		   default=0,
		   help="Submit jobs starting from startNumber.",
		   metavar="jobs")
 parser.add_option("-s","--sleeptime",
                   type="int",
                   dest="safetySleep",
		   default=120,
		   help="Wait sleepTime seconds between trials.",
		   metavar="time")
 parser.add_option("-j","--jobtemplate",
                   type="string",
                   dest="jobTemplate",
		   default="jobtemplate.cmd",
		   help="jobTemplate is the template for the jobs to be submitted.",
		   metavar="file")
 parser.add_option("-x","--expid",
                   type="string",
                   dest="expid",
     default="truc",
     help="Experiment ID to be run (needed for the creation of Joblist and scripts.")
 parser.add_option("-c","--clean",
                   action="store_true",
		   dest="clean",
                   help="Clean generated jobscript files when submitted.")
 parser.add_option("-v","--verbose",
                   action="store_true",
		   dest="verbose",
                   help="Verbose mode.")
 parser.add_option("-d","--debug",
                   action="store_true",
		   dest="debug",
                   help="Debug mode, display the jobs to be submitted.")
 parser.add_option("-r","--restart",
                   action="store_true",
		   dest="restart",
                   help="restart from joblist pickled")
 (options,args) = parser.parse_args()
 return (options,args)

##TODO add an option to pass the expid on the command line.
## TODO pass jobtemplate as an optional argument to CreateJobScript

def log_long(message):
 print "[%s] %s" % (time.asctime(),message)
 
def log_short(message):
 d = time.localtime()
 date = "%04d-%02d-%02d %02d:%02d:%02d" % (d[0],d[1],d[2],d[3],d[4],d[5])
 print "[%s] %s" % (date,message)

def getActiveJobs():
 return queueStatus.get('active')

def getWaitingJobs():
 return int(queueStatus.get('eligible'))+int(queueStatus.get('blocked'))

def goToSleep():
 log_short("Going to sleep (%s seconds) before retry..." % options.safetySleep)
 time.sleep(options.safetySleep)

def updateQueueStatus():
 output = commands.getoutput('mnq | grep -E "eligible job|blocked job|active job" | grep -v "\-.*"').split('\n')
 for line in output:
  numberOfJobs = line.split()[0]
  typeOfJobs = line.split()[1]
  queueStatus[typeOfJobs] = numberOfJobs

def submitJob(scriptName):
 jobid=0
 if not options.debug:
  jobid=parse_mnq.submitJob(scriptName)
 else:
  os.system('bash %s' % scriptName)
  jobid=random.randrange(0,1000000)
 return jobid

def handler(signum,frame,joblist):
 if signum == signal.SIGHUP:
  smart_stop()
 if signum == signal.SIGINT:
  normal_stop()

def normal_stop():
 # Must stop autosubmit cancelling all the jobs currently running.
 jobTable = mnq.getMyJobs()
 joblist=pickle.load(file('../auxfiles/joblist.pkl','r'))
 for key in jobTable.keys():
  #os.system('mncancel %s' % jobTable.get(key)[0])
  jobname=jobTable.get(key)[0]
  print 'mncancel %s' % jobname
  if options.debug:
   os.system('kill %s' % jobname)
  else:
   parse_mnq.cancelJob(jobTable.getId(jobname))
  ##TODO: check that jobtable.get(key)[0] is the name of the job
  
  job=JobListFactory.getName(joblist, jobname)
  ##TODO this should have a special status
  job.setStatus(job.Status.FAILED)
 completed=JobListFactory.getCompleted(joblist)
 failed=JobListFactory.getFailed(joblist)
 notrun=JobListFactory.getNotInQueue(joblist)
 JobListFactory.saveJobList(completed,'joblist_completed.pkl')
 JobListFactory.saveJobList(failed,'joblist_failed.pkl')
 JobListFactory.saveJobList(notrun,'joblist_notrun.pkl')
 sys.exit(0)

def smart_stop():
 message('Stopping, and checking submitted jobs and pickle them!!!')
 joblist=pickle.load(file('../auxfiles/joblist.pkl','r'))
 queuing=JobListFactory.getQueuing(joblist)
 JobListFactory.cancelJobList(queuing)
 for job in queuing:
  job.setStatus(job.Status.READY)
 ready=JobListFactory.getReady(joblist)
 JobListFactory.saveJobList(ready,'joblist_notrun.pkl')

 runningjobs=JobListFactory.getRunning(joblist)
 while runningjobs.__len__() !=0:
  message('There are still jobs running!!!')
  JobListFactory.checkjobInList(runningjobs)
  time.sleep(SLEEPING_TIME)
   
 completed=JobListFactory.getCompleted(joblist)
 failed=JobListFactory.getFailed(joblist)
 JobListFactory.saveJobList(completed,'joblist_completed.pkl')
 JobListFactory.saveJobList(failed,'joblist_failed.pkl')
 sys.exit(SUCCESS)

#################################
# AUXILIARY FUNCTIONS
#################################
def has_jobs():
 #output = commands.getoutput('mnq --xml')
 output = commands.getoutput('cat mnq.xml')
 if output.find('job') == -1:
  return False
 else:
  return True

def message(msg):
 print "%s" % msg

####################
# Main Program
####################
if __name__ == "__main__":
 
 os.system('clear')
 signal.signal(signal.SIGHUP,handler)#,joblist)
 
 (options,args)=handle_options()
 ## expid="scal" 
 log_short("Jobs to submit: %s" % options.totalJobs)
 log_short("Start with job number: %s" % options.alreadySubmitted)
 log_short("Maximum waiting jobs in queues: %s" % options.maxWaitingJobs)
 log_short("Sleep: %s" % options.safetySleep)
 log_short("Starting job submission...")
 
 alreadySubmitted=options.alreadySubmitted
 totalJobs=options.totalJobs 
 myTemplate=options.jobTemplate
 expid=options.expid
 print "My template name is: %s" % myTemplate
 print "The Experiment name is: %s" % expid
 
 if options.restart:
  joblist=pickle.load(file('../auxfiles/joblist.pkl','r'))
  log_short("Restarting from joblist pickled in ../auxfiles/joblist.pkl")
 else: 
  joblist=userdefinedfunctions.CreateJobList(expid)
 
 #joblist=JobListFactory.CreateJobList2()
 print "Length of joblist: ",len(joblist)
 totaljobs=len(joblist)
 log_short("New Jobs to submit: "+str(totaljobs)) 
 # Main loop. Finishing when all jobs have been submitted
 while JobListFactory.getNotInQueue(joblist).__len__()!=0 :
  updateQueueStatus()
  waiting = getWaitingJobs()
  active = getActiveJobs()
  available = options.maxWaitingJobs-waiting
  print "saving joblist"
  JobListFactory.saveJobList(joblist,'../auxfiles/joblist.pkl')
  graphname=joblist[0].getExpid()+'_graph.png'
  if  (os.path.exists(graphname)):
   pathname='/gpfs/projects/ecm86/common/db'
   if  (os.path.exists(pathname)):
    os.system('cp %s %s' % (graphname,pathname))
  if options.verbose:
   log_short("Active jobs in queues:\t%s" % active)
   log_short("Waiting jobs in queues:\t%s" % waiting)

  if available == 0:
   if options.verbose:
    log_short("There's no room for more jobs...")
  else:
   if options.verbose:
    log_short("We can safely submit %s jobs..." % available)
  
  #get the list of jobs currently in the Queue
  jobinqueue=JobListFactory.getInQueue(joblist)
  print "number of jobs in queue :%s" % jobinqueue.__len__() 
  JobListFactory.checkjobInList(jobinqueue)
   
  ##after checking the jobs , no job should have the status "submitted"
  ##Jordi throw an exception if this happens (warning type no exit)
  if (JobListFactory.getReady(joblist).__len__()!=0) :
   JobListFactory.printJobs(JobListFactory.getReady(joblist))
   
  #update joblist
  #we only need to update the active jobs
  JobListFactory.updateJobList(joblist)
  activejobs=JobListFactory.getActive(joblist)
  print "in the factory there is %s active jobs" % activejobs.__len__()

  ## get the list of jobs READY
  jobsavail=JobListFactory.getReady(joblist)
  if (min(available,len(jobsavail)) ==0):
   print "There is no job READY or available"
   print "Number of job ready: ",len(jobsavail)
   print "Number of jobs available in queue:", available
  else: 
   ##should sort the jobsavail by priority Clean->post->sim>ini
   JobListFactory.printJobs(jobsavail)
   jobsavail=JobListFactory.sortByType(jobsavail)
   
   jobsavail.reverse()
   for job in jobsavail[0:min(available,len(jobsavail))]:
    scriptname=userdefinedfunctions.CreateJobScript(job) 
    print scriptname
    jobid=submitJob(scriptname)
    job.setId(jobid)
    ##set status to "submitted"
    job.setStatus(2)
    if options.clean:
     os.system("rm %s" % scriptname)

    alreadySubmitted += 1
     
   log_short("We have already submitted %s of %s jobs" % (alreadySubmitted,totaljobs))
  
  if JobListFactory.getActive(joblist).__len__()!=0:
   goToSleep()
 
 log_short("Finished job submission")
 JobListFactory.printJobs(joblist)
 if options.verbose:
  updateQueueStatus()
  waiting = getWaitingJobs()
  active = getActiveJobs()
  log_short("Active jobs in queues:\t%s" % active)
  log_short("Waiting jobs in queues:\t%s" % waiting)

