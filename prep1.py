#!/opt/python27/bin/python

__author__ = 'zhuyund'

# for a partition
# 0. find big shards
# 1. split shardmaps
# 2. gen jobs: extid -> intid
# 3. gen jobs: intid -> docVec

import argparse
import os, sys

parser = argparse.ArgumentParser()
parser.add_argument("partition_name")
parser.add_argument("shardmaps_dir")
parser.add_argument("repo_dir")
args = parser.parse_args()

base_dir = "/bos/usr0/zhuyund/partition/SplitShards/output/" + args.partition_name
print base_dir


if not os.path.exists(base_dir):
    os.makedirs(base_dir)

# big shards
thresholds = 1000000
if not os.path.isfile(args.shardmaps_dir + "/size"):
    print "no size file!"
    sys.exit(-1)

size_file = open(args.shardmaps_dir + "/size")
f = open(base_dir + "/shard", "w") # write splitted shard ids into this file
for line in size_file:
    line = line.strip()
    size, shard = line.split()
    shard = shard.split('/')[-1]
    if shard == "total" or shard == "size":
        continue
    size = int(size)
    f.write(shard + " " + str(size/100000 + 1) + '\n')
    print size
    if size < thresholds:
        continue
    f.write(shard + '\n')
    if not os.path.exists(base_dir + "/" + shard):
        os.makedirs(base_dir+"/"+shard)

    # split
    extid_dir = base_dir+"/"+shard + "/extid/"
    print extid_dir
    if not os.path.exists(extid_dir):
        os.makedirs(extid_dir)
    print "spliting extid. shard: " + shard
    #os.system("./splitShardMap.py {0} {1}".format(args.shardmaps_dir+"/"+shard, extid_dir))

    # gen extid->intid jobs
    intid_dir = base_dir+"/"+shard + "/intid/"
    job_dir = base_dir+"/"+shard + "/jobs/"
    if not os.path.exists(job_dir):
        os.makedirs(job_dir)
    job_file_path = job_dir + "extid2intid.job"

    os.system("./genExtid2IntidJobs.py {0} {1} {2} {3} {4}".format(args.repo_dir, extid_dir, intid_dir, size/100000 + 1, job_file_path))
    print "extid2intid job wrote to " + job_file_path

    # gen intid->docvec jobs
    dv_dir = base_dir + "/" + shard + "/docvec/"
    job_file_path = job_dir + "dumpVectors.job"
    os.system("./genDumVectorJobs.py {0} {1} {2} {3} {4}".format(args.repo_dir, intid_dir, dv_dir, size/100000 + 1, job_file_path))
    print "dumpVectors job wrote to " + job_file_path

size_file.close()

f.close()





