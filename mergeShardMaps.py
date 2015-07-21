#!/opt/python27/bin/python

__author__ = 'zhuyund'

import argparse
import os, sys
import jobWriter


parser = argparse.ArgumentParser()
parser.add_argument("partition_name")
parser.add_argument("org_shardmaps_dir")
args = parser.parse_args()

base_dir = "/bos/usr0/zhuyund/partition/SplitShards/output/" + args.partition_name
print base_dir

shardmap_dir = base_dir + "/shardMap/"
if not os.path.exists(shardmap_dir):
    os.makedirs(shardmap_dir)

f = open(base_dir + "/shard") # splitted shard ids and numbers
splitted_shards = {}
for line in f:
    shard, num, size = line.split()
    splitted_shards[shard] = int(num)


if not os.path.isfile(args.org_shardmaps_dir + "/size"):
    print "no size file!"
    sys.exit(-1)
size_file = open(args.org_shardmaps_dir + "/size")

shard_id = 1

for line in size_file:
    line = line.strip()
    size, shard = line.split()
    shard = shard.split('/')[-1]
    if shard == "total" or shard == "size":
        continue
    if shard not in splitted_shards:
        cmd = "cp {0}/{1} {2}/{3}".format(args.org_shardmaps_dir, shard, shardmap_dir, shard_id)
        os.system(cmd)
        shard_id += 1
    else:
        for s in range(1, splitted_shards[shard] + 1):
            cmd = "cp {0}/{1}/shardMap/{2} {3}/{4}".format(base_dir, shard, s, shardmap_dir, shard_id)
            os.system(cmd)
            shard_id += 1

size_file.close()

f.close()




