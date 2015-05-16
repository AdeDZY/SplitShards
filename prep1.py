#!/opt/python27/bin/python

__author__ = 'zhuyund'

# for a partition
# 0. find big shards
# 1. split shardmaps
# 2. extid -> intid

import argparse
import os, sys

parser = argparse.ArgumentParser()
parser.add_argument("partition_name")
parser.add_argument("shardmaps_dir")
parser.add_argument("repo_dir")
args = parser.parse_args()

base_dir = "./output/" + args.partition_name

if not os.path.exists(base_dir):
    os.makedirs(base_dir)

# big shards
thresholds = 700000
if not os.path.isfile(args.shardmaps_dir + "/size"):
    print "no size file!"
    sys.exit(-1)

size_file = open(args.shardmaps_dir + "/size")
for line in size_file:
    line = line.strip()
    size, shard = line.split()
    size = int(size)
    if size < thresholds:
        continue
    os.makedirs(base_dir+"/"+shard)

    # split
    extid_dir = base_dir+"/"+shard + "/extid/"
    if not os.path.exists(extid_dir):
        os.makedirs(exit())
    print "spliting extid. shard: " + shard
    os.system("./splitShardMap.py {0} {1}".format(args.shardmaps_dir+"/"+shard, extid_dir))

    # gen extid->intid jobs
    intid_dir = base_dir+"/"+shard + "/intid/"
    job_dir = base_dir+"/"+shard + "/jobs/"
    if not os.path.exists(job_dir):
        os.makedirs(job_dir)
    job_file_path = job_dir + "extid2intid.job"

    os.system("./genExtid2IntidJobs.py {0} {1} {2} {3} {4}".format(args.repo_dir, extid_dir, intid_dir, size/100000 + 1, job_file_path))
    print "extid2intid job wrote to " + job_file_path

size_file.close()







