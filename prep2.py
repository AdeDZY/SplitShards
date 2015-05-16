#!/opt/python27/bin/python

__author__ = 'zhuyund'

# for a partition
# 1. sample docs
# 2. gen kmeans jobs
# 3. gen inference jobs

import argparse
import os, sys


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
parser.add_argument("repo_dir")
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

    # number of clusters
    ncluster = get_ncluster(size)

    # gen clustering job





f.close()




