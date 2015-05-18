#!/opt/python27/bin/python
import string
import sys, os
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("inferResDir", help="director containing inference results")
parser.add_argument("shardNum",type = int, help="the file of externel IDs")
parser.add_argument("outputDir", help="output director")
parser.add_argument("-d","--docNum", type=int, default=50220423, help="number of docs. default: clueweb B 50220423")
args = parser.parse_args()

inferResDir = args.inferResDir
extidPath = "/bos/usr0/zhuyund/partition/cw09catB/02-DocVectors/all.extid" 
shardNum = args.shardNum 
outputDir = args.outputDir 
docNum = args.docNum 
splitSize = 100000

extidFile = open(extidPath, 'r')
startID = 1
intidPrev = 0

shardMap = [list() for i in range(0, shardNum)]

while startID < docNum:
	endID = startID + splitSize - 1
	if endID > docNum:
		endID = docNum
	inferFilePath = inferResDir + '/cw09catB_' + str(startID) + '-' + str(endID) + '.inference'
	print inferFilePath
	inferFile = open(inferFilePath, 'r')
	
	nLine = 0
	for line in inferFile:
		items = [int(item) for item in line.split(':')]
		intid = items[0] + startID - 1
		shardID = items[1]

		# in case of missing docs
		while intidPrev < intid:
			intidPrev = intidPrev + 1
			extid = extidFile.readline().strip()

		shardMap[shardID - 1].append(extid)
		nLine = nLine + 1
		if nLine >= splitSize:
			break
	inferFile.close()
	startID = endID + 1

if not os.path.exists(outputDir):
	os.makedirs(outputDir)

for i in range(0, shardNum):
	outFile = open(outputDir+'/'+str(i + 1),'w')
	for extid in shardMap[i]:
		outFile.write(extid + '\n')

extidFile.close()
outFile.close()


