#!/opt/python27/bin/python
import os, sys
import string
import time
import argparse
import random

parser = argparse.ArgumentParser()
parser.add_argument("samplingTrial")
parser.add_argument("numClusters")
parser.add_argument("iterCount")
parser.add_argument("clusteringTrial")
parser.add_argument("weights", nargs=5, help="title heding url inlink body", type=float)
parser.add_argument("seeds")
args = parser.parse_args()
print args

baseDir = '/bos/usr0/zhuyund/partition/cw09catB/'

#corresponding to different sampling trials
datFile = baseDir+'/02-SampleDocs/SampledDV/cw09catB_sampleTrial'+args.samplingTrial+'.dat'
outputDir = baseDir + '/03-Kmeans/samplingTrial' + args.samplingTrial + '/'

lamda = '0.1'
minVocabSeed = '100'

trialDir = outputDir + args.numClusters + 'Clusters-'+args.iterCount+'Iters-'+args.clusteringTrial+'/'
centroidDir = trialDir+'centroids/'

print outputDir
if not os.path.exists(outputDir):
	print outputDir
	os.makedirs(outputDir)

if not os.path.exists(trialDir):
	print trialDir 
	os.makedirs(trialDir)

if not os.path.exists(centroidDir):
	print centroidDir
	os.makedirs(centroidDir)

param_file = open(trialDir+'/param', 'w')
for k, v in vars(args).items():
	param_file.write(k + ':' + str(v) + '\n')
param_file.close()


logFile = trialDir+'log'
cmd = "/bos/tmp11/zhuyund/partition/Clustering-field/kmeans "+datFile+" "+args.numClusters+" "+lamda+" "+args.iterCount+" "+centroidDir+" "+minVocabSeed+" " + '5 '
for i in range(5):
	cmd += str(args.weights[i]) + " " 
cmd += " " + args.seeds + " >& " + logFile
r = random.random() * 60
time.sleep(int(r))
os.system(cmd)




