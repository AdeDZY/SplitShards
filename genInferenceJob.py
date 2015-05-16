#!/opt/python27/bin/python
import string
import sys, os
import argparse

parser = argparse.ArgumentParser()

parser.add_argument("datFileDir")
parser.add_argument("clusterTrialDir")
parser.add_argument("numPartstr")
parser.add_argument("lambstr")
parser.add_argument("outputFilePrefix",help="output file prefix")
args = parser.parse_args()

nDoc = 50220423
splitSize = 100000 

centroidDirPath = args.clusterTrialDir + '/centroids'
inferenceDirPath = args.clusterTrialDir + '/inference'
if not os.path.exists(inferenceDirPath):
	os.makedirs(inferenceDirPath)

param_file = open(args.clusterTrialDir + '/param')
for line in param_file:
	k,v = line.split(':')
	if k == 'weights':
		weights = v[1:-2].split(',')
param_file.close()

startID = 1

head = 'Universe = vanilla'
head = head + '\nExecutable = /bos/tmp11/zhuyund/partition/Inference-field/inference'
head = head + '\n\n' + 'Log = /tmp/zhuyund_inference.log'
head = head +  '\nOutput = log/inference.out'
head = head + '\nError = log/inference.err\n'

nJob = 0
startID = 1
f = open('tmp.f','w')
while startID < nDoc:
	endID = startID + splitSize - 1
	if endID > nDoc:
		endID = nDoc
	inferenceFilePath = inferenceDirPath+'/' + 'cw09catB_'+ str(startID) + '-' + str(endID) + '.inference'
	datFilePath = args.datFileDir + '/' + 'cw09catB_'+ str(startID) + '-' + str(endID) + '.dat'
	if nJob%50== 0:
		f.close()
		f = open(args.outputFilePrefix+'_'+str(nJob/50)+'.job','w')
		f.write(head)
	cmd = 'Arguments = ' + datFilePath + ' ' + centroidDirPath + ' ' + inferenceFilePath + ' ' + args.numPartstr + ' ' + args.lambstr + ' 5 ' 
	for w in range(5):
		cmd += ' ' + (weights[w])
	f.write(cmd)
	f.write('\nQueue'+'\n\n')
	nJob = nJob + 1
	startID = endID + 1

