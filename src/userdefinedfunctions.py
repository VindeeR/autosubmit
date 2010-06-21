#!/usr/bin/env python

import JobListFactory
from Job import *


def CreateJobScript2(job,parameters):
 mytemplate="template"
 scriptname=job.getName()+'.cmd'
 splittedname=job.getName().split('_')
 #procx=int(splittedname[1])
 parameters['NEMOPROCX']=splittedname[1]
 #procy=int(splittedname[2])
 parameters['NEMOPROCY']=splittedname[2]
 parameters['JOBNAME'] = job.getName() 
 ##update parameters
 print "*************My Template: %s" % mytemplate
 templateContent = file(mytemplate).read()
 for key in parameters.keys():
  print "KEY: %s\n" % key
  if key in templateContent:
   print "%s:\t%s" % (key,parameters[key])
  templateContent = templateContent.replace(key,parameters[key])
 
 file(scriptname,'w').write(templateContent)
 return scriptname

def CreateJobScript3(job,parameters):
 scriptname=job.getName()+'.cmd'
 template="../templates/dummy"
 splittedname=scriptname.split('_')
 #date=int(splittedname[1])
 parameters['DATE']=splittedname[1]
 #member=int(splittedname[2])
 parameters['MEMBER']=splittedname[2]
 chunk=int(splittedname[3])
 parameters['CHUNK']=splittedname[3]
 parameters['JOBNAME'] = job.getName() 
 if (job.getJobType()==0):
  print "jobType:", job.getJobType()
  mytemplate=template+'.sim'
  ##update parameters
 elif (job.getJobType()==1):
  print "jobType:", job.getJobType()
  mytemplate=template+'.post'
  ##update parameters
 elif (job.getJobType()==2):
  print "jobType:", job.getJobType()
  ##update parameters
  mytemplate=template+'.clean'
 else: 
  print "Unknown Job Type"
  
 print "*************My Template: %s" % mytemplate
 templateContent = file(mytemplate).read()
 for key in parameters.keys():
  print "KEY: %s\n" % key
  if key in templateContent:
   print "%s:\t%s" % (key,parameters[key])
   templateContent = templateContent.replace(key,parameters[key])
  
 file(scriptname,'w').write(templateContent)
 return scriptname

def CreateJobScript1(job,parameters):
 scriptname=job.getName()+'.cmd'
 template="Template"
 splittedname=scriptname.split('_')
 #date=int(splittedname[1])
 parameters['DATE']=splittedname[1]
 #member=int(splittedname[2])
 parameters['MEMBER']=splittedname[2]
 chunk=int(splittedname[3])
 parameters['CHUNK']=splittedname[3]
 parameters['JOBNAME'] = job.getName() 
 prev=[0,59,59+61,59+61*2,59+61*2+62,59+61*3+62]
 if (job.getJobType()==0):
  print "jobType:", job.getJobType()
  mytemplate=template+'.sim'
  ##update parameters
  parameters['WALLCLOCKLIMIT']='72:00:00'
  parameters['PREV']=str(24*prev[chunk-1])
 elif (job.getJobType()==1):
  print "jobType:", job.getJobType()
  mytemplate=template+'.post'
  ##update parameters
  parameters['WALLCLOCKLIMIT']="02:01:00"
 elif (job.getJobType()==2):
  print "jobType:", job.getJobType()
  ##update parameters
  mytemplate=template+'.clean'
  parameters['WALLCLOCKLIMIT']="10:00:00"
 else: 
  print "Unknown Job Type"
  
 print "*************My Template: %s" % mytemplate
 templateContent = file(mytemplate).read()
 for key in parameters.keys():
  print "KEY: %s\n" % key
  if key in templateContent:
   print "%s:\t%s" % (key,parameters[key])
   templateContent = templateContent.replace(key,parameters[key])
  
 file(scriptname,'w').write(templateContent)
 return scriptname


def CreateJobList1(list_of_dates, num_member,num_chunks):
 joblist=list()
 #param=dict()
 for dates in list_of_dates:
  for mem in range(num_member):
   for chk in range(1,num_chunks+1):
    job_rootname="job_"+str(dates)+'_'+str(mem)+'_'+str(chk)+'_'
    job_sim = Job(job_rootname+'sim',0,Job.Status.WAITING,Job.JobType.SIMULATION)
    job_post = Job(job_rootname+'post',0,Job.Status.WAITING,Job.JobType.POSTPROCESSING)
    job_clean = Job(job_rootname+'clean',0,Job.Status.WAITING,Job.JobType.CLEANING)
    #set depency of postprocessing jobs
    job_post.setParents([job_sim.name])
    job_post.setChildren([job_clean.name])
    #set Parents of clean job
    job_clean.setParents([job_post.name])
    #set first child of sim job
    job_sim.setChildren([job_post])
    ##Set status of first chunk to READY
    if (chk==1):
     job_sim.setStatus(Job.Status.READY)
     job_sim.setParents([])
    if(chk>1):
     parentname="job_"+str(dates)+'_'+str(mem)+'_'+str(chk-1)+'_'+'sim'
     job_sim.setParents([parentname])
     if (chk>2):
      parentname="job_"+str(dates)+'_'+str(mem)+'_'+str(chk-2)+'_'+'clean'
      job_sim.addParents(parentname)
    if (chk<num_chunks):
     childname="job_"+str(dates)+'_'+str(mem)+'_'+str(chk+1)+'_'+'sim'
     job_sim.addChildren(childname)
    if (chk<num_chunks-1):
     childname="job_"+str(dates)+'_'+str(mem)+'_'+str(chk+2)+'_'+'sim'
     job_clean.setChildren([childname])

    JobListFactory.printJobs([job_sim ,job_post ,job_clean])
    joblist+=[job_sim ,job_post ,job_clean]
  
 JobListFactory.updateGenealogy(joblist)
 return joblist   
    
def CreateJobList2():
 joblist=list()
 #param=dict()
 proc_list=([1,1],[4,4],[2,2],[4,2],[2,4],[2,1],[1,2],[4,1],[1,4])
 for proc in proc_list:
  print "PROC %s %s" %(proc[0],proc[1])
  job_rootname="nemo-comp_"+str(proc[0])+'_'+str(proc[1])
  job_comp = Job(job_rootname,0,Job.Status.READY,Job.JobType.SIMULATION)
  #no parents nor children
  job_comp.setParents([])
  job_comp.setChildren([])
  JobListFactory.printJobs([job_comp])
  joblist+=[job_comp]

 JobListFactory.updateGenealogy(joblist)
 return joblist   


def generateJobParameters1(expid):
 # Table which contains all the keys to change in the template.
 # Feel free to add new variables
 parameters = dict()
 
 # Configure jobname and shell type
 parameters['FILENAME'] = "job-exp-%s.cmd" % expid
 parameters['SHELL'] = "/bin/bash"

 # Configure job directives...
 parameters['JOBNAME'] = "job-" 
 parameters['OUTFILE'] = "job-%s.out" % '%j'
 parameters['ERRFILE'] = "job-%s.err" % '%j'
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
    
def GenerateParameter(expid):
 if expid=="yves":
  print "creatig the parameters for the experiment: %s" % expid
  parameter=generateJobParameters1(expid)
  return parameter
 else:
  print "there is no defined generateparameter function for the expid : %s " % expid 

def CreateJobScript(expid,job):
 if expid=="yves":
  print "creating the script for job: %s" % job.getName()
  parameter=generateJobParameters1(expid)
  scriptname=CreateJobScript1(job,parameter)
  return scriptname
 elif expid=="yve1":
  print "creating the script for job: %s" % job.getName()
  parameter=generateJobParameters1(expid)
  scriptname=CreateJobScript3(job,parameter)
  return scriptname
 else:
  print "there is no defined CreateJobScript function for the expid: %s" % expid 

def CreateJobList(expid):
 if expid=="yves":
  print "Creating the joblist for experiment: %s" % expid
  dates=[1990]
  members=5
  numchuncks=6 
  joblist=CreateJobList1(dates,members,numchuncks)
  return joblist
 elif expid=="yve1":
  print "Creating the joblist for experiment: %s" % expid
  dates=[1990]
  members=5
  numchuncks=10 
  joblist=CreateJobList1(dates,members,numchuncks)
  return joblist
 else:
  print "there is no defined CreateJoblist  function for the expid: %s" % expid 


