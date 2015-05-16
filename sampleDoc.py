#!/opt/python27/bin/python
import argparse
import random

parser = argparse.ArgumentParser()
parser.add_argument("sampleTrial")
parser.add_argument("--sampleRate","-r", type=float, default=0.01)
args = parser.parse_args()
indexSize = 50220423
splitSize = 100000
startID = 1 

random.seed()
inputDvDir= '/bos/tmp11/zhuyund/partition/cw09catB/02-DocVectors/DV/'
outputDvFile = open('/bos/tmp11/zhuyund/partition/cw09catB/02-SampleDocs/SampledDV/cw09catB_sampleTrial{0}.dat'.format(args.sampleTrial), 'w')
outputIDFile = open('/bos/tmp11/zhuyund/partition/cw09catB/02-SampleDocs/SampledDV/sampledDocID_cw09catB_sampleTrial{0}'.format(args.sampleTrial), 'w')
while startID < indexSize:
	endID = startID + splitSize - 1
	if endID > indexSize:
		endID = indexSize
	dvFile = open('{0}/cw09catB_{1}-{2}.dat'.format(inputDvDir, startID, endID))
	id = startID
	for line in dvFile:
		if random.random() <= args.sampleRate:
			outputDvFile.write(line)
			outputIDFile.write(str(id)+'\n')	
		id = id + 1
	dvFile.close()
	startID = endID + 1

outputDvFile.close()
outputIDFile.close()

