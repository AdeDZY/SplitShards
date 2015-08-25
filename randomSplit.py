#!/opt/python27/bin/python

__author__ = 'zhuyund'


import argparse
import os, sys
import jobWriter
import random


def get_ncluster(shard_size):
    """
    number of clusters
    :param sahrd_size:
    :return: int
    """

    n_cluster = 2
    aim = 500000.0

    if shard_size < aim:
        return 1

    while True:
        if float(shard_size)/n_cluster - aim < aim - float(shard_size)/(n_cluster + 1):
            break
        n_cluster += 1
    return n_cluster



parser = argparse.ArgumentParser()
parser.add_argument("partition_name")
parser.add_argument("org_shardmaps_dir")
args = parser.parse_args()

base_dir = "/bos/usr0/zhuyund/partition/SplitShards/output/" + args.partition_name
print base_dir

f = open(base_dir + "/shard") # splitted shard ids and numbers
for line in f:
    line = line.strip()
    shard, num, size = line.split()
    num = int(num)
    size = int(size)
    if shard == "total" or shard == "size":
        continue

    if not os.path.exists("{0}/{1}/shardMap_rand/".format(base_dir, shard)):
        os.makedirs("{0}/{1}/shardMap_rand/".format(base_dir, shard))

    n_cluster = get_ncluster(size)
    shards = [[] for i in range(n_cluster)]
    extid_file = open("{0}/{1}".format(args.org_shardmaps_dir, shard))
    for exitd in extid_file:
        r = random.randint(1, n_cluster)
        shards[r - 1].append(exitd)
    extid_file.close()
    for i in range(1, n_cluster + 1):
        out_file = open("{0}/{1}/shardMap_rand/{2}".format(base_dir, shard, i), 'w')
        for exitd in shards[i - 1]:
            out_file.write(exitd )
        out_file.close()

f.close()




