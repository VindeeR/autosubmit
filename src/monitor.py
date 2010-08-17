#!/usr/bin/env python

import commands
import parse_mnq
import JobListFactory
import pickle
import parseMnqXml as mnq
import time
import matplotlib.pyplot as plt

def pie_chart(completed,failed,ready,running,queuing,count):
 
 # make a square figure and axes
 fig=plt.figure(1, figsize=(6,6))
 fig.clear()
 fig.add_axes([0.1, 0.1, 0.8, 0.8])
 labels = 'completed','failed','ready','running','queuing'
 fracs = [completed,failed,ready,running,queuing]
 explode=(0, 0.05, 0, 0, 0)
 plt.pie(fracs, explode=explode, labels=labels, autopct='%1.1f%%', shadow=True)
 plt.title('Job Status', bbox={'facecolor':'0.8', 'pad':5})
 filename="test"+str(count)+'.png'
 fig.savefig(filename)
 #show()

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
 count=0
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
 while count<5:
  print "%s completed job out of %s total jobs!" %(completed,total)
  print "%s completed, %s running, %s queuing, %s submitted, %s ready, %s waiting" %(completed,running,queuing,submitted,ready,waiting)
  print "%s queuing jobs" % queuing
  pie_chart(completed,failed,ready,running,waiting,count)
  time.sleep(3)
  count=count+1
  status_list=[waiting,ready,submitted,queuing,running,completed]
  a=range(status_list.__len__()-1)
  a.reverse()
  for i in a:
   if status_list[i]>0:
    status_list[i]=status_list[i]-1
    status_list[i+1]=status_list[i+1]+1
  completed=status_list[5]
  running=status_list[4]
  queuing=status_list[3]
  submitted=status_list[2]
  ready=status_list[1]
  waiting=status_list[0]
  finished=JobListFactory.getFinished(jobs).__len__() 
 file1.close()
