#!/usr/bin/env python

import commands
import pydot
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

def CreateTree(expid,dates,members,chunks):
 graph=pydot.Dot(graph_type='digraph')
 ndate=list()
 node_a = pydot.Node(expid, style="filled", fillcolor="red")

 for date in dates:
  newnode=pydot.Node(str(date), style="filled", fillcolor="green")
  ndate+=[newnode]
  graph.add_edge(pydot.Edge(node_a, newnode))
  #for mem in range(members):
  # edge2=pydot.Edge(str(date),str(mem))
  # graph.add_edge(edge2)
  # edge3=pydot.Edge(str(mem),'ini')
  # graph.add_edge(edge3)
 
 graph.write_png('myexample3_graph.png')

def ColorStatus(status):
 color='white'
 if status==0:
   color='blue'
 elif status==1:
   color='navy'
 elif status==2:
   color='purple'
 elif status==3:
   color='yellow'
 elif status==4:
   color='orange'
 elif status==5:
   color='green'
 elif status==-1:
   color='red'
 return color

def CreateTreeList(joblist):
 graph=pydot.Dot(graph_type='digraph')
 expid=joblist[0].getExpid()
 for job in joblist:
  node_job = pydot.Node(job.getName(),shape='box', style="filled", fillcolor=ColorStatus(job.getStatus()))
  graph.add_node(node_job)
  #graph.set_node_style(node_job,shape='box', style="filled", fillcolor=ColorStatus(job.getStatus()))
  if job.hasChildren()!=0:
   for child in job.getChildren():
    node_child=pydot.Node(child.getName() ,shape='box', style="filled", fillcolor=ColorStatus(child.getStatus()))
    graph.add_node(node_child)
    #graph.set_node_style(node_child,shape='box', style="filled", fillcolor=ColorStatus(job.getStatus()))
    graph.add_edge(pydot.Edge(node_job, node_child))
 pngfile=expid+'_graph.png'
 pdffile=expid+'_graph.pdf'
 #graph.set_graphviz_executables({'dot': '/gpfs/apps/GRAPHVIZ/2.26.3/bin/dot'})
 graph.write_png(pngfile) 
 #graph.write_pdf(pdffile) 
 
def dummy_list(jobs):
 count=0
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

if __name__ == "__main__":
 filename='../auxfiles/joblist_yve2.pkl'
 file1=open(filename,'r')
 jobs=pickle.load(file(filename,'r'))
 #dummy_list(jobs)
 #for job in jobs:
 # job.setExpid('ploum')
 CreateTreeList(jobs)
 file1.close()
