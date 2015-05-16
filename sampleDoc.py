#!/opt/python27/bin/python
import argparse
import random

parser = argparse.ArgumentParser()
parser.add_argument("name")
parser.add_argument("shard")
parser.add_argument("n_split_files", type=int)
parser.add_argument("sampleRate", type=float)
args = parser.parse_args()

random.seed()

baseDir = '/bos/usr0/zhuyund/partition/SplitShards/output/' + args.name + '/' + args.shard
inputDvDir=  baseDir + '/docvec/'
outputDvFile = open(baseDir+ '/sampled.dat', 'w')
outputIDFile = open(baseDir + '/sampledID', 'w')
for i in range(1, args.n_split_files + 1):
    dvFile = open('{0}/{1}.dat'.format(inputDvDir, i))
    for line in dvFile:
        if random.random() <= args.sampleRate:
            outputDvFile.write(line)
            outputIDFile.write(str(id)+'\n')
        id = id + 1
    dvFile.close()

outputDvFile.close()
outputIDFile.close()
print "sampled docvec wrote to " + baseDir+ '/sampled.dat!'
