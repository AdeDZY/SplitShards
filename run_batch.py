#!/opt/python27/bin/python
import string
import time
import os, sys
import argparse
import pushover
import datetime

parser = argparse.ArgumentParser()
parser.add_argument("jobFilePrefix",help="job file prefix")
parser.add_argument("nBatch",type=int,help="total number of batches")
parser.add_argument("--nStart","-s",type=int,help="start batch number",default=0)
parser.add_argument("jobName",help="job name in condor")
args = parser.parse_args()

query = "condor_q zhuyund | grep " + "\""+args.jobName + "\"" + "| wc -l"
limit = 53
for i in range(args.nStart,args.nStart + args.nBatch):
	while True:
		out = os.popen(query)
		nRunning = int(out.readline())
		print nRunning
		if datetime.datetime.now().hour < 9:
			limit = 3
		else:
			limit = 3
		if nRunning <= limit:
			break
		else:
			time.sleep(120)
	submitCommand = "condor_submit " +args.jobFilePrefix + '_' + str(i) + '.job'
	os.system(submitCommand)
	print submitCommand

pushover.pushover("run_dvCreation finish!")
	



