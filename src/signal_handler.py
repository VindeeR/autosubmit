#!/usr/bin/env python

import signal, time, sys
import commands
import parseMnqXml as mnq

SLEEPING_TIME = 10
SUCCESS = 0
FAILURE = 1

#################################
# MAIN FUNCTIONS
#################################
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

#################################
# MAIN METHOD
#################################
if __name__ == "__main__":

 # Declaring signals and catching them
 signal.signal(signal.SIGHUP,handler)
 signal.signal(signal.SIGINT,handler)
 
 while True:
  print 'Processing!!!!'
  time.sleep(SLEEPING_TIME)
