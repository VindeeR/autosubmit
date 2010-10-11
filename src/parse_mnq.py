#!/usr/bin/env python

import os, commands
import time
import logging
import Job

JOB_NAME = 0
JOB_ID = 1
JOB_STATUS = 3
JOB_PROCS = 4
JOB_REMAINING_TIME = 5
JOB_START_TIME = 6

job_logger = logging.getLogger("AutoLog.parse_mnq")

def mnq_preprocess():
 #calling mnq and output it in MNQ...
 os.system('mnq > MNQ')
 # Getting the line numbers where the jobs start
 starting_numbers = commands.getoutput("cat MNQ | grep -n NAME | cut -f1 -d':'")
 # Getting the line numbers where the jobs finish
 ending_numbers = commands.getoutput("cat MNQ | grep -n -E '^[0-9]-*' | cut -f1 -d':'")
 # Splitting the lines in order to obtain a list
 list_starting = starting_numbers.split()
 list_ending = ending_numbers.split()

 list_start_index = list()
 list_end_index = list()

 # Preprocess of the list numbers
 for number in list_starting:
  result = int(number)+2
  list_start_index.append(result)
 for number in list_ending:
  result = int(number)-1
  list_end_index.append(result)
 
 #print list_starting
 #print list_ending
 
 # Retrieving the job lines
 jobs_lines = list() 
 for index in range(len(list_start_index)):
  jobs_lines.append(commands.getoutput("cat MNQ | sed -n -e '%d,%dp' | grep -v '^$' | tr -s ' '" % (list_start_index[index],list_end_index[index])))
  print commands.getoutput("cat MNQ | sed -n -e '%d,%dp' | grep -v '^$' | tr -s ' '" % (list_start_index[index],list_end_index[index]))
 return jobs_lines
 
def store_job(jobs_lines):

 job_table = dict()
 for jobs in jobs_lines:
  # How many jobs are in jobs var
  jobs_list = jobs.split('\n')

  for job in jobs_list:
   if not len(job):
    break
   params = job.split()
   job_table[params[JOB_ID]] = params[JOB_NAME],\
                               params[JOB_STATUS],\
			       params[JOB_PROCS],\
			       params[JOB_REMAINING_TIME],\
			       " ".join(['%s' % word for word in params[JOB_START_TIME:]])
 return job_table

def checkjobs(job_dict,job_ids):
 if not len(job_ids):
  return

 job_id_list = list()
 if type(job_ids) is not list:
  job_id_list.append(job_ids)
 else:
  job_id_list = job_ids
  
 status_list = dict()
 for job_id in job_id_list:
  status_list[job_id] = job_dict[job_id][1]
  
 return status_list
  
def checkjob(job_id):
 stat_num=0
 if type(job_id) is not int:
  print 'The argument %s is not an integer!!!' % job_id
  return
 state=""
 while state.split(':').__len__()!=2:
  output = commands.getoutput('checkjob %s' % str(job_id))
  #output = commands.getoutput('cat CHECKJOB_SAMPLE')
  state = output[output.find('State'):output.find('\n',output.find('State'))].strip()
  print "Getting the status of %s !" % str(job_id) 
  print state
  ## if state is still empty , we need to give it more time
  if state.split(':').__len__()!=2:
   print "we will retry in 5 sec"
   time.sleep(5)
 
 status=state.split(':')[1].strip()
 if (status.upper()=='COMPLETED'):
  stat_num=5
 elif (status.upper()=='RUNNING'):
  stat_num=4
 elif (status.upper()=='PENDING' or status.upper()=='IDLE') or status.upper()=='BLOCKED':
  stat_num=3
 elif (status.upper()=='FAILED' or status.upper()=='NODE_FAIL' or status.upper()=='TIMEOUT'):
  stat_num=-1
 else:
  print 'UNKNOWN STATUS: %s' % status
 
 return stat_num 

def get_name(job_id):
 if type(job_id) is not int:
  print 'The argument %s is not an integer!!!' % job_id
  return
 output = commands.getoutput('checkjob %s' % str(job_id))
 #output = commands.getoutput('cat CHECKJOB_SAMPLE')
 name = output[output.find('AName'):output.find('\n',output.find('AName'))].strip()
 return name.split(':')[1].strip(' "')

def submitJob(jobname):
 output=commands.getoutput('mnsubmit %s ' % jobname)
 print output
 print output.split(" ")[3]
 #print output.split(":")[1].strip().split()[3]
 job_id=output.split(" ")[3]
 #.strip().split()[3]
 return int(job_id)

def cancelJob(job_id):
 os.system("mncancel %s" % str(job_id))

def main():
 print 'HELLO we are calling MNQ_preprocess....'
 jobid=submitJob("MonthlyRestart")
 print "just submitted job number: %s" % str(jobid)
 print "now cancelling..."
 cancelJob(jobid)
 lines_of_jobs = mnq_preprocess()
 print 'sizeof job_lines: ', lines_of_jobs.__len__() 
 job_table = store_job(lines_of_jobs)
 for item in job_table.keys():
  print type(item)
  print item,job_table[item]
  print checkjob(int(item))
  print get_name(int(item))
  print checkjob(int(item))


if __name__ == "__main__":
 main()
