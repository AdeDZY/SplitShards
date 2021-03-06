#!/opt/python27/bin/python

import os
import time
import argparse
import random

parser = argparse.ArgumentParser()
parser.add_argument("name", help="run name. eg. cw-s1c1")
parser.add_argument("shard", help="shard to split. eg. 1")
parser.add_argument("n_clusters")
parser.add_argument("iter_count")
parser.add_argument("qw", type=float)
parser.add_argument("--constant", "-c", type=int, default=0, help="1 = use constant weight")
parser.add_argument("--ref_threshold", "-r", type=float, default=1.0)
parser.add_argument("--dataset", "-d", default="cwb", help="cwb gov2")
args = parser.parse_args()
print args

baseDir = '/bos/usr0/zhuyund/partition/SplitShards/output/' + args.name + '/' + args.shard

# corresponding to different sampling trials
datFile = baseDir+'/sampled.dat'

minVocabSeed = '10'

trialDir = baseDir + '/kmeans/'
centroidDir = trialDir+'centroids/'

if not os.path.exists(trialDir):
    print trialDir
    os.makedirs(trialDir)

if not os.path.exists(centroidDir):
    print centroidDir
    os.makedirs(centroidDir)


logFile = trialDir+'log'
cmd = "/bos/usr0/zhuyund/partition/Clustering-qweight/qweightKmeans.sh "\
      + datFile + " " + args.n_clusters + " " + str(args.qw) + " " + args.iter_count\
      + " " + centroidDir + " " + minVocabSeed + " " + '1 ' + '1 '
if args.dataset == "cwb":
    cmd += " " + "selectSeeds" + " field " + "/bos/usr0/zhuyund/partition/Clustering-qweight/data/aol-first2.int_df "\
       + str(args.ref_threshold) + " " + str(args.constant) + " >& " + logFile
elif args.dataset == "gov2":
    #cmd += " " + "selectSeeds" + " field " + "/bos/usr0/zhuyund/partition/Clustering-qweight/data/aol-first2.int_df_gov2 "\
    #cmd += " " + "selectSeeds" + " field " + "/bos/usr0/zhuyund/partition/Clustering-qweight/gov2_inlink.int_df_less "\
    cmd += " " + "selectSeeds" + " field " + "/bos/usr0/zhuyund/partition/Clustering-qweight/gov2.all "\
       + str(args.ref_threshold) + " " + str(args.constant) + " >& " + logFile
r = random.random() * 60
time.sleep(int(r))
os.system(cmd)




