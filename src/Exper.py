#!/usr/bin/env python

import logging
import os
import sys
from ConfigParser import SafeConfigParser
import cfuConfigParser


logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(name)s %(levelname)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    filename='exper.log',
                    filemode='w')

exper_logger = logging.getLogger("ExperLog")


class Exper:
 """Class to handle all the tasks with Experiments."""
    
 class Status:
  """Class to handle the status of a Experiment"""
  NOTCREATED = -1
  BUILDING =0
  EXISTS = 1
  COMPLETED = 2 

 class ExpType:
  """Class to handle the type of a Expver.
      A the moment contains only 1 type:
      ECEARTH experiments consisting in multiple start dates, members and chunks."""
  ECEARTH=1
  DUMMY=0
 
 def __init__(self,expid,exptype):
  self.longname = 'long name not defined'
  self.expid = expid
  self.status = 0
  self.Exptype = 1
  self.joblist = list()
  self.parser = SafeConfigParser()

 def getName(self):
  return self.longname
 
 def getId(self):
  return self.expid
 
 def getStatus(self):
  return self.status
 
 def getExpType(self):
  return self.Exptype

 def getExpid(self):
  return self.expid
 
 def getParser(self):
  return self.parser
 
 def getJobList(self):
  return self.joblist

 
 def printExper(self,log=0):
  if log:
   exper_logger.info("%s\t%s\t%s" % ("Expid Name","ExpId","Exp Status"))
   exper_logger.info("%s\t\t%s\t%s" % (self.longname,self.expid,self.status))
  else:
   print "%s\t%s\t%s" % ("Expid Name","ExpId","Exp Status")
   print "%s\t\t%s\t%s" % (self.longname,self.expid,self.status)

 def setName(self,newName):
  self.longname = newName
 
 def setId(self,newId):
  self.expid = newId
 
 def setStatus(self,newStatus):
  self.status = newStatus
  
 def setExpid(self,newexpid):
  self.expid = newexpid

 def setParser(self,newparser):
  self.parser = newparser


 def setExpType(self,newtype):
  self.exptype = newtype

##TODO
# Load/save experiment from pickle first then database
# Copy experiment and assign new expid (see jordi's functions)
# Rules to modify exp (once completed, it cannot be modified!)
# Rules to include it in the database: requirement of an experiment description, user etc.
 def setup(self):
  #from parser set list of dates, members and chunks
  #dates=[]
  #members=[]
  #chunks=[]
  #from JobListFactory create the joblist
  #joblist=jlf.CreateJobList(dates,members,chunks)


  #
  pass


if __name__ == "__main__":
 mainExper = Exper('uno','0001',Exper.Status.BUILDING,Exper.ExpType.ECEARTH)
 mainExper.printExper()
 print mainExper.getName()
 print mainExper.getExpid()
 print mainExper.getStatus()
 parser = SafeConfigParser()
 if(len(sys.argv) != 2):
  print "Error missing config file"
 else:
  parser=cfuConfigParser.cfuConfigParser(sys.argv[1])
 
 mainExper.setParser(parser)
 print mainExper.getParser().get('config','hpcarch')



