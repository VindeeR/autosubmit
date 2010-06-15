#!/usr/bin/env python
from optparse import OptionParser
import time, os, sys
import commands
import parse_mnq
import JobListFactory
import signal
import parseMnqXml as mnq

####################
# Global Variables
####################

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
 (options,args) = parser.parse_args()
 return (options,args)


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
  os.system('cat %s' % scriptName)
 return jobid

def generateJobParameters():
 # Table which contains all the keys to change in the template.
 # Feel free to add new variables
 parameters = dict()
 
 # Useful variables
 # Which job are we working with
 whoAmI = options.alreadySubmitted
 # How many jobs there are
 dirNumber = int(whoAmI/100)
 expid = 'test'
 
 # Configure jobname and shell type
 parameters['FILENAME'] = "job-exp-%s-%s.cmd" % (expid,whoAmI)
 parameters['SHELL'] = "/bin/bash"

 # Configure job directives...
 parameters['JOBNAME'] = "job-%s" % whoAmI
 parameters['OUTFILE'] = "outputs/dir%s/job-%s-%s.out" % (dirNumber,whoAmI,'%j')
 #parameters['ERRFILE'] = "outputs/dir%s/job-%s-%s.err" % (dirNumber,whoAmI,'%j')
 parameters['INITIALDIR'] = "."
 #parameters['TOTALTASKS'] = "1"
 parameters['WALLCLOCKLIMIT'] = "00:05:00"
 parameters['EXPID']=expid
 # Variables specific to the experiment
 #parameters['TOTALMEMBERNUM']="1"
 parameters['MEMBER']="1"
 parameters['DATES']='1990'
# parameters['TOTALNUMSIMULATION']="1"
# parameters['RUNLENGTH']="10"
# parameters['CHUNKLENGTH']="1"
 parameters['CHUNK']='1'
 # Variables to identify the job itself
 #parameters['ASWHOAMI']= str(whoAmI)
 #parameters['ASHOWMANY']= str(howMany)
 #parameters['ASDIRNUMBER']= str(dirNumber)

 # Variables specifics to the run of ECEARTH
# parameters['ECEARTH']="/gpfs/projects/ecm86/ecm86503/ecearth2.1"
# parameters['TESTDATADIR']="/gpfs/projects/ecm86/common/testdata"
#parameters['SCRIPTDIR']="/gpfs/projects/ecm86/common/testrun/scripts/common"
# parameters['WRITINGDIR']="/gpfs/scratch/ecm86/ecm86503/TestsResults/%s" % expid

# Variables specifics to IFS
# parameters['IFS_resolution']="T159L62"
# parameters['IFSDIR']='ECEARTH'
# parameters['IFS_nproc']='28'
# parameters['IFS_nprocv']='1'
 
# parameters['IFS_EXE']='IFSDIR'+"/ifs/bin/ifsMASTER"
# Variables specifics to NEMO
#parameters['NEMO_nprocX']= '4'
#parameters['NEMO_nprocY']= '4'
 

 # Configure the job body (what to do)
 parameters['BODY'] = "hostname"

 return parameters
 
def generateJobParameters2():
 # Table which contains all the keys to change in the template.
 # Feel free to add new variables
 parameters = dict()
 
 # Useful variables
 #expid = 'nemo_comp'
 
 # Configure jobname and shell type
 parameters['SHELL'] = "/bin/bash"
 # Variables specifics to NEMO
 parameters['NEMOPROCX']= '4'
 parameters['NEMOPROCY']= '4'
 

 # Configure job directives...
 parameters['JOBNAME'] = "nemo_comp-%s-%s" % (parameters['NEMOPROCX'],parameters['NEMOPROCY'])
 parameters['INITIALDIR'] = "/gpfs/projects/ecm86/ecm86503/ecearth2.1/build"

 return parameters

def handler(signum,frame):
 if signum == signal.SIGHUP:
  smart_stop()
 if signum == signal.SIGINT:
  normal_stop()

def normal_stop():
 # Must stop autosubmit cancelling all the jobs currently running.
 jobTable = mnq.getMyJobs()
 for key in jobTable.keys():
  #os.system('mncancel %s' % jobTable.get(key)[0])
  print 'mncancel %s' % jobTable.get(key)[0]
 sys.exit(0)

def smart_stop():
 message('Stopping, and checking submitted jobs and pickle them!!!')
 while has_jobs():
  message('There are jobs still running!!!')
  time.sleep(SLEEPING_TIME) 
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
 signal.signal(signal.SIGHUP,handler)
 (options,args)=handle_options()
 
 log_short("Jobs to submit: %s" % options.totalJobs)
 log_short("Start with job number: %s" % options.alreadySubmitted)
 log_short("Maximum waiting jobs in queues: %s" % options.maxWaitingJobs)
 log_short("Sleep: %s" % options.safetySleep)
 log_short("Starting job submission...")
 
 alreadySubmitted=options.alreadySubmitted
 totalJobs=options.totalJobs 
 myTemplate=options.jobTemplate
 dates=[1990]
 members=5
 parameters = generateJobParameters2()
 #for a decadal run we do 10 chuncks of 1 year
 numchuncks=6 
 #initialise the job list of lenght totaljobs
 joblist=JobListFactory.CreateJobList(dates,members,numchuncks)
 #joblist=JobListFactory.CreateJobList2()
 print "Length of joblist: ",len(joblist)
 totaljobs=len(joblist)
 log_short("New Jobs to submit: "+str(totaljobs)) 
 # Main loop. Finishing when all jobs have been submitted
 while JobListFactory.getActive(joblist).__len__()!=0 :
  updateQueueStatus()
  waiting = getWaitingJobs()
  active = getActiveJobs()
  available = options.maxWaitingJobs-waiting
   
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
  for job in jobinqueue:
   job.printJob()
   status=parse_mnq.checkjob(job.getId())
   if(status==5):
    job.check_completion()
   else:
    job.setStatus(status)
  ##after checking the jobs , no job should have the status "submitted"
  ##Jordi throw an exception if this happens (warning type no exit)
  if (JobListFactory.getReady(joblist).__len__()!=0) :
   JobListFactory.printJobs(JobListFactory.getReady(joblist))
   
  #update joblist
  #we only need to update the active jobs
  activejobs=JobListFactory.getActive(joblist)
  JobListFactory.updateJobList(activejobs)
  
  ## get the list of jobs READY
  jobsavail=JobListFactory.getReady(activejobs)
  if (min(available,len(jobsavail)) ==0):
   print "There is no job READY or available"
   print "Number of job ready: ",len(jobsavail)
   print "Number of jobs available in queue:", available
  else: 
   ##should sort the jobsavail by priority Clean->post->sim
   JobListFactory.printJobs(jobsavail)
   jobsavail=JobListFactory.sortByStatus(jobsavail)
   
   jobsavail.reverse()
   for job in jobsavail[0:min(available,len(jobsavail))]:
    scriptname=job.CreateJobScript(myTemplate,parameters) 
    jobid=submitJob(scriptname)
    job.setId(jobid)
    ##set status to "submitted"
    job.setStatus(2)
    if options.debug:
     job.setStatus(5)
  
    if options.clean:
     os.system("rm %s" % scriptname)

    alreadySubmitted += 1
     
   log_short("We have already submitted %s of %s jobs" % (alreadySubmitted,totalJobs))
  
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

