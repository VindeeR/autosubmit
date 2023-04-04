#!/usr/bin/env python2
###############################################################################
#              a000_ASThread_16801803863908_5_60
###############################################################################
#
#SBATCH -J a000_ASThread_16801803863908_5_60
#SBATCH --qos=debug
#SBATCH -A bsc32
#SBATCH --output=a000_ASThread_16801803863908_5_60.out
#SBATCH --error=a000_ASThread_16801803863908_5_60.err
#SBATCH -t 02:00:00
#SBATCH --cpus-per-task=1
#SBATCH -n 8
###############################################################################

import os
import sys
# from bscearth.utils.date import date2str
from threading import Thread
from commands import getstatusoutput
from datetime import datetime
import time
from math import ceil
from collections import OrderedDict
import copy


class Unbuffered(object):
    def __init__(self, stream):
        self.stream = stream

    def write(self, data):
        self.stream.write(data)
        self.stream.flush()

    def writelines(self, datas):
        self.stream.writelines(datas)
        self.stream.flush()

    def __getattr__(self, attr):
        return getattr(self.stream, attr)


sys.stdout = Unbuffered(sys.stdout)
wrapper_id = "8aQlI6U962_FAILED"
# Defining scripts to be run
scripts = [[u'a000_19600101_fc0000_1_SIM.cmd', u'a000_19600101_fc0000_2_SIM.cmd', u'a000_19600101_fc0000_3_SIM.cmd',
            u'a000_19600101_fc0000_4_SIM.cmd', u'a000_19600101_fc0000_5_SIM.cmd'], [u'a000_19600101_fc0000_POST.cmd'],
           [u'a000_19600101_fc0001_4_SIM.cmd', u'a000_19600101_fc0001_2_SIM.cmd', u'a000_19600101_fc0001_5_SIM.cmd',
            u'a000_19600101_fc0001_1_SIM.cmd', u'a000_19600101_fc0001_3_SIM.cmd'], [u'a000_19600101_fc0001_POST.cmd'],
           [u'a000_19600101_fc0002_4_SIM.cmd', u'a000_19600101_fc0002_2_SIM.cmd', u'a000_19600101_fc0002_1_SIM.cmd',
            u'a000_19600101_fc0002_5_SIM.cmd', u'a000_19600101_fc0002_3_SIM.cmd'], [u'a000_19600101_fc0002_POST.cmd'],
           [u'a000_19600101_fc0003_4_SIM.cmd', u'a000_19600101_fc0003_3_SIM.cmd', u'a000_19600101_fc0003_5_SIM.cmd',
            u'a000_19600101_fc0003_1_SIM.cmd', u'a000_19600101_fc0003_2_SIM.cmd'], [u'a000_19600101_fc0003_POST.cmd'],
           [u'a000_19600101_fc0004_5_SIM.cmd', u'a000_19600101_fc0004_1_SIM.cmd', u'a000_19600101_fc0004_4_SIM.cmd',
            u'a000_19600101_fc0004_3_SIM.cmd', u'a000_19600101_fc0004_2_SIM.cmd'], [u'a000_19600101_fc0004_POST.cmd'],
           [u'a000_19600101_fc0005_2_SIM.cmd', u'a000_19600101_fc0005_5_SIM.cmd', u'a000_19600101_fc0005_1_SIM.cmd',
            u'a000_19600101_fc0005_4_SIM.cmd', u'a000_19600101_fc0005_3_SIM.cmd'], [u'a000_19600101_fc0005_POST.cmd'],
           [u'a000_19600101_fc0006_5_SIM.cmd', u'a000_19600101_fc0006_2_SIM.cmd', u'a000_19600101_fc0006_4_SIM.cmd',
            u'a000_19600101_fc0006_1_SIM.cmd', u'a000_19600101_fc0006_3_SIM.cmd'], [u'a000_19600101_fc0006_POST.cmd'],
           [u'a000_19600101_fc0007_5_SIM.cmd', u'a000_19600101_fc0007_1_SIM.cmd', u'a000_19600101_fc0007_4_SIM.cmd',
            u'a000_19600101_fc0007_2_SIM.cmd', u'a000_19600101_fc0007_3_SIM.cmd'], [u'a000_19600101_fc0007_POST.cmd'],
           [u'a000_19600101_fc0008_1_SIM.cmd', u'a000_19600101_fc0008_3_SIM.cmd', u'a000_19600101_fc0008_5_SIM.cmd',
            u'a000_19600101_fc0008_4_SIM.cmd', u'a000_19600101_fc0008_2_SIM.cmd'], [u'a000_19600101_fc0008_POST.cmd'],
           [u'a000_19600101_fc0009_1_SIM.cmd', u'a000_19600101_fc0009_5_SIM.cmd', u'a000_19600101_fc0009_4_SIM.cmd',
            u'a000_19600101_fc0009_2_SIM.cmd', u'a000_19600101_fc0009_3_SIM.cmd'], [u'a000_19600101_fc0009_POST.cmd']]


class JobThread(Thread):
    def __init__(self, template, id_run):
        Thread.__init__(self)
        self.template = template
        self.id_run = id_run
        self.fail_count = 0

    def run(self):
        jobname = self.template.replace('.cmd', '')
        print("Thread level {0}".format(jobname))
        # os.system("echo $(date +%s) > "+jobname+"_STAT")
        out = str(self.template) + ".out." + str(self.fail_count)
        err = str(self.template) + ".err." + str(self.fail_count)
        print(out + "\n")
        print("{1}/machinefiles/machinefile_{0}".format(jobname,os.getcwd()))
        os.environ["MACHINEFILE"] = "{1}/machinefiles/machinefile_{0}".format(jobname,os.getcwd())
        command = "./" + str(self.template) + " " + str(self.id_run) + " " + os.getcwd()
        (self.status) = getstatusoutput(command + " > " + out + " 2> " + err)


class JobListThread(Thread):
    def __init__(self, jobs_list, id_run, node_list):
        Thread.__init__(self)
        self.jobs_list = jobs_list
        self.id_run = id_run
        self.node_list = node_list

    def run(self):
        pid_list = []
        print("Jobs list: {0}".format(self.jobs_list))
        print("len_jobs_list: {0}".format(len(self.jobs_list)))
        print("all_nodes: {0}".format(self.node_list))

        for i,job in enumerate(self.jobs_list):
            jobname = job.replace(".cmd", '')
            section = jobname.split('_')[-1]
            machines = ""
            cores = int(jobs_resources[section]['PROCESSORS'])
            tasks = int(jobs_resources[section]['TASKS'])
            processors_per_node = int(jobs_resources['PROCESSORS_PER_NODE'])
            nodes = int(ceil((float(tasks*cores)) / processors_per_node))
            print("Nodes: {0}".format(nodes))
            print("Nodes_remaining_for_this_list: {0}".format(self.node_list))
            while nodes > 0:
                node = self.node_list.pop(0)
                machines += "{0} 1\n".format(node, processors_per_node)
                nodes = nodes - 1
                # machines = "\n".join([s for s in machines.split("\n") if s])
            #machines = "\n".join([s for s in machines.split("\n") if s])
            print("FINAL_MACHINES:{0} ".format(machines))
            with open("machinefiles/machinefile_" + jobname, "w") as machinefile:
                machinefile.write(machines)
            current = JobThread(job, i + self.id_run)
            pid_list.append(current)
            current.start()

        # Waiting until all scripts finish
        for i in range(len(pid_list)):
            pid = pid_list[i]
            pid.join()

            completed_filename = self.jobs_list[i].replace('.cmd', '_COMPLETED')
            completed_path = os.path.join(os.getcwd(), completed_filename)
            failed_filename = self.jobs_list[i].replace('.cmd', '_FAILED')
            failed_path = os.path.join(os.getcwd(), failed_filename)
            failed_wrapper = os.path.join(os.getcwd(), wrapper_id)
            if os.path.exists(completed_path):
                print datetime.now(), "The job ", pid.template, " has been COMPLETED"
            else:
                open(failed_wrapper, 'w').close()
                open(failed_path, 'w').close()
                print datetime.now(), "The job ", pid.template, " has FAILED"


# Getting the list of allocated nodes
os.system("scontrol show hostnames $SLURM_JOB_NODELIST > {0}".format(node_id))
os.system("mkdir -p machinefiles")

with open('node_list_{0}'.format(wrapper_id), 'r') as file:
    all_nodes = file.read()

all_nodes = all_nodes.split("\n")

total_cores = 5
jobs_resources = {u'POST': {'TASKS': u'12', 'PROCESSORS': '1'}, 'MACHINEFILES': u'STANDARD',
                  'PROCESSORS_PER_NODE': u'12', u'SIM': {'TASKS': '1', 'PROCESSORS': '1'}}
processors_per_node = int(jobs_resources['PROCESSORS_PER_NODE'])
idx = 0
all_cores = []
while total_cores > 0:
    if processors_per_node > 0:
        processors_per_node -= 1
        total_cores -= 1
        all_cores.append(all_nodes[idx])
    else:
        idx += 1
        processors_per_node = int(jobs_resources['PROCESSORS_PER_NODE'])
processors_per_node = int(jobs_resources['PROCESSORS_PER_NODE'])

failed_wrapper = os.path.join(os.getcwd(), wrapper_id)
for i in range(len(scripts)):
    current = JobListThread(scripts[i], i * (len(scripts[i])), copy.deepcopy(all_cores))
    current.start()
    current.join()
    if os.path.exists(failed_wrapper):
        os.system("rm -f node_list_{0}".format(wrapper_id))
        os.remove(os.path.join(os.getcwd(), wrapper_id))
        wrapper_failed = os.path.join(os.getcwd(), "WRAPPER_FAILED")
        open(wrapper_failed, 'w').close()
        os._exit(1)
os.system("rm -f {0}".format(node_list))


