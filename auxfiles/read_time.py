import commands
import time 
import pylab
import matplotlib

errfiles=commands.getoutput('ls *.err').split()
three_hourly=[]
six_hourly=[]
ntasks_list=[]
ntasks_list2=[]
for file in errfiles:
 print "filename:%s " % file
 splittedname=file.split('_')
 nemo_procx=splittedname[6]
 nemo_procy=splittedname[7]
 coup_freq=splittedname[9]
 ifs_procx=splittedname[3]
 ifs_procy=splittedname[4]
 ifs_nproc=int(ifs_procx)*int(ifs_procy)
 ntasks=int(nemo_procx)*int(nemo_procy)+ifs_nproc+1
 print "Total task %s at %s h coupling frequency" % (ntasks,coup_freq)
 mytime=commands.getoutput('tail -3 %s |head -1' % file).split()[1].split('.')[0]
 mysec=int(mytime.split('m')[0])*60+int(mytime.split('m')[1])
 print "My time is: ",mysec
 if coup_freq=='3':
  three_hourly.append(mysec)
  ntasks_list.append(ntasks)
 elif coup_freq=='6':
  six_hourly.append(mysec)
  ntasks_list2.append(ntasks)
print "length of 3h-coup: ", len(three_hourly)
print "length of 6h-coup: ", len(six_hourly)
fig=pylab.figure(1, figsize=(6,6))
labels = '3h-coupling','6h-coupling'
pylab.plot(ntasks_list,three_hourly,'r')
pylab.plot(ntasks_list2,six_hourly,'b')
pylab.title('Scalability', bbox={'facecolor':'0.8', 'pad':5})
fig.savefig('scalability333.png')
print ntasks_list
print three_hourly
print ntasks_list2
print six_hourly
print "that is it!" 
