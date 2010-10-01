#!/usr/bin/env python

import time

####################
# Global Variables
####################
filename='logfile.log'

def set_logfile_name(newname):
 filename=newname
  
def log_long(message):
 file(filename,'a').writelines("[%s] %s" % (time.asctime(),message))
 
def log_short(message):
 d = time.localtime()
 date = "%04d-%02d-%02d %02d:%02d:%02d" % (d[0],d[1],d[2],d[3],d[4],d[5])
 file(filename,'a').writelines("[%s] %s" % (date,message))


if __name__ == "__main__":
 set_logfile_name("dummy")
 log_long("Hellooooo")
 log_short("ECHOOOO")