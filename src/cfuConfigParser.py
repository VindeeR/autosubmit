#!/usr/bin/env python

from ConfigParser import SafeConfigParser
import sys
import os.path

invalid_values = False
#def ccpLoadDefaults(parser):
#	 parser.add_section('config')
#	 parser.set('config', 'MAXWAITINGJOBS', '50')
#	 parser.set('config', 'TOTALJOBS', '1000')
#	 parser.set('config', 'ALREADYSUBMITTED', '0')
#	 parser.set('config', 'JOBTEMPLATE', 'jobtemplate.cmd')
#	 parser.set('config', 'VERBOSE', 'true')
#	 parser.set('config', 'DEBUG', 'false')
#	 parser.set('config', 'RUNMODE', 'remote')
#	 parser.set('config', 'AUTOFILEDIR', 'AUTOSUB_WORKING_COPY/tmp')

def ccpCheckValues(value, valid_values):
	global invalid_values

	if(value.lower() not in valid_values): 
		print "Invalid value: " + value
		invalid_values = True

def cfuConfigParser(file):
	hpcarch = ['marenostrum', 'ithaca']
	runmode = ['local', 'remote']
	loglevel = ['debug', 'info', 'warning', 'error', 'critical']
	
	#option that must be in config file and has no default value
	mandatory_opt = ['expid', 'clean', 'restart', 'hpcusername', 'hpcfiledir']
	
	# default value in case this options does not exist on config file
	default = ({'MAXWAITINGJOBS' : '50', 'TOTALJOBS': '1000', 'ALREADYSUBMITTED': '0', 'JOBTEMPLATE': 'jobtemplate.cmd', 'VERBOSE': 'true', 'DEBUG': 'false', 'RUNMODE': 'remote', 'AUTOFILEDIR': 'AUTOSUB_WORKING_COPY/tmp'})

	# check file existance
	if(not os.path.isfile(file)):
		print "File does not exist"
		sys.exit()

	# load default values
	parser = SafeConfigParser(default)
	#ccpLoadDefaults(parser)
	parser.read(file)

	# check which options of the mandatory one are not in config file
	missing = list(set(mandatory_opt).difference(parser.options('config')))
	if(missing):
		print "Missing options"
		print missing
		sys.exit()
	
	ccpCheckValues(parser.get('config', 'hpcarch'), hpcarch)
	ccpCheckValues(parser.get('config', 'runmode'), runmode)
	ccpCheckValues(parser.get('config', 'loglevel'), loglevel)
	#if(parser.get('config', 'hpcarch').lower() not in hpcarch):
		#print "HPCARCH value incorrect: " + parser.get('config', 'hpcarch')
	
	#if(parser.get('config', 'runmode').lower() not in runmode):
		#print "RUNMODE value incorrect: " + parser.get('config', 'runmode')

	#if(parser.get('config', 'loglevel').lower() not in loglevel):
		#print "LOGLEVEL value incorrect: " + parser.get('config', 'loglevel')


	print parser.items('config')
	if(invalid_values):
		print "\nInvalid config file"
		sys.exit()
	else:
		print "\nConfig file OK"
	return parser


def experConfigParser(file):
	# default value in case this options does not exist on config file
	default = ({'EXPID' : 'dumi', 'TYPE': '1', 'STATUS': '0', 'LONGNAME': 'Just a test')

	# check file existance
	if(not os.path.isfile(file)):
		print "File does not exist"
		sys.exit()

	# load default values
	parser = SafeConfigParser(default)
	parser.read(file)
	print parser.items('expdef')
	return parser

if __name__ == "__main__":
	if(len(sys.argv) != 2):
		print "Error missing config file"
	else:
		cfuConfigParser(sys.argv[1])
