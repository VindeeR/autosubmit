#!/usr/bin/env python

import JobListFactory
import chunk_date_lib
from Job import *
import monitor
import pickle
import userdefinedfunctions as udf

def restarting_script(name):
 parameters=udf.generateJobParameters_yve2('yve2')
 scriptname=name+'.cmd'
 mytemplate="../templates/RestartChunk.sim"
 splittedname=name.split('_')
 parameters['SDATE']=splittedname[1]
 string_date=splittedname[1]
 parameters['MEMBER']=splittedname[2]
 total_chunk=int(parameters['Chunk_NUMBERS'])
 chunk_length_in_month=int(parameters['Chunk_SIZE_MONTH'])
 parameters['CHUNK']=splittedname[3]
 chunk=int(splittedname[3])
 parameters['JOBNAME'] = name 
 chunk_start_date=chunk_date_lib.chunk_start_date(string_date,chunk,chunk_length_in_month)
 chunk_end_date=chunk_date_lib.chunk_end_date(chunk_start_date,chunk_length_in_month)
 run_days=chunk_date_lib.running_days(chunk_start_date,chunk_end_date)
 prev_days=chunk_date_lib.previous_days(string_date,chunk_start_date)
 chunk_end_days=chunk_date_lib.previous_days(string_date,chunk_end_date)
 day_before=chunk_date_lib.previous_day(string_date)
 chunk_end_date_1=chunk_date_lib.previous_day(chunk_end_date)

 chunkprev_start_date=chunk_date_lib.chunk_start_date(string_date,chunk-1,chunk_length_in_month)
 chunkprev_end_date=chunk_date_lib.chunk_end_date(chunkprev_start_date,chunk_length_in_month)
 chunkprev_end_date_1=chunk_date_lib.previous_day(chunkprev_end_date)
 
 parameters['DAY_BEFORE']=day_before
 parameters['Chunk_START_DATE']=chunkprev_start_date
 parameters['Chunk_END_DATE']=chunkprev_end_date_1
 parameters['RUN_DAYS']=str(run_days)
 parameters['Chunk_End_IN_DAYS']=str(chunk_end_days)

 chunk_start_month=chunk_date_lib.chunk_start_month(chunk_start_date)
 chunk_start_year=chunk_date_lib.chunk_start_year(chunk_start_date)
 
  
 parameters['Chunk_START_YEAR']=str(chunk_start_year)
 parameters['Chunk_START_MONTH']=str(chunk_start_month)
 if total_chunk==chunk:
  parameters['Chunk_LAST']='.TRUE.'
 else:
  parameters['Chunk_LAST']='.FALSE.'
 
 parameters['PREV']=str(prev_days)
  
 print "*************My Template: %s" % mytemplate
 templateContent = file(mytemplate).read()
 for key in parameters.keys():
  print "KEY: %s\n" % key
  if key in templateContent:
   print "%s:\t%s" % (key,parameters[key])
   templateContent = templateContent.replace(key,parameters[key])
  
 file(scriptname,'w').write(templateContent)
 return scriptname


if __name__ == "__main__":
 #joblist=pickle.load(file('../auxfiles/joblist_yve2.pkl','r'))
 namelist=['job_19751101_0_1_ini','job_19751101_0_1_sim','job_19751101_0_1_post']
 #jobs=[j for j in joblist if namelist.__contains__(j.getName())]
 parameters= dict()
 parameters['Chunk_NUMBERS']='1'
 parameters['Chunk_SIZE_MONTH']='4'
 parameters['INITIALDIR']='/home/ecm86/ecm86503/testpatch'
 parameters['LOGDIR']='/home/ecm86/ecm86503/testpatch'
 parameters['EXPID']='yve2'
 parameters['VERSION']='v2.2.2'
 joblist=udf.CreateJobList('testpatch')
 for job in joblist:
  name=udf.CreateJobScript_yve2(job,parameters)
  print name
 #for name in namelist:
 # scriptname= restarting_script(name)
 # print scriptname
