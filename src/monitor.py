#!/usr/bin/env python

import commands
import parse_mnq
import JobListFactory
import pickle
import parseMnqXml as mnq
import time
from pylab import *

def pie_chart(completed,failed,ready,running,queuing):
 
 # make a square figure and axes
 fig=figure(1, figsize=(6,6))
 ax = axes([0.1, 0.1, 0.8, 0.8])
 labels = 'completed','failed','ready','running','queuing'
 fracs = [completed,failed,ready,running,queuing]
 explode=(0, 0.05, 0, 0, 0)
 pie(fracs, explode=explode, labels=labels, autopct='%1.1f%%', shadow=True)
 title('Job Status', bbox={'facecolor':'0.8', 'pad':5})
 fig.savefig('test3.png')
 

def show_serie():
 x=range(10)
 for a in x:
  pie_chart(1+a,2,3,4,5)
  time.sleep(10)
  
if __name__ == "__main__":
 filename='../auxfiles/joblist.pkl'
 file1=open(filename,'r')
 total=10
 finished=0
 while finished!=total:
  jobs=pickle.load(file(filename,'r'))
  total=jobs.__len__()
  completed=JobListFactory.getCompleted(jobs).__len__()
  finished=JobListFactory.getFinished(jobs).__len__()
  failed=JobListFactory.getFailed(jobs).__len__()
  running=JobListFactory.getRunning(jobs).__len__()
  ready=JobListFactory.getReady(jobs).__len__()
  waiting=JobListFactory.getWaiting(jobs).__len__()
  submitted=JobListFactory.getSubmitted(jobs).__len__()
  queuing=JobListFactory.getQueuing(jobs).__len__()
  print "%s completed job out of %s total jobs!" %(completed,total)
  print "%s queuing jobs" % queuing
  pie_chart(completed,failed,ready,running,waiting)
  time.sleep(3)
# file1.close()
