#!/usr/bin/env python

import time

####################
# Global Variables
####################

class logging:
 
 
 def get_logfile_name(self):
  return self.filename
 def set_logfile_name(self,newname):
  self.filename=newname

  
 def log_long(self,message):
  file(self.filename,'a').writelines("[%s] %s\n" % (time.asctime(),message))
 
 def log_short(self,message):
  d = time.localtime()
  date = "%04d-%02d-%02d %02d:%02d:%02d" % (d[0],d[1],d[2],d[3],d[4],d[5])
  file(self.filename,'a').writelines("[%s] %s\n" % (date,message))


if __name__ == "__main__":
 fn = logging()
 fn.set_logfile_name("dummy.log")
 print fn.get_logfile_name()
 fn.log_long("Hellooooo")
 fn.log_short("ECHOOOO")