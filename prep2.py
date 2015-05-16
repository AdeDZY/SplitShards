#!/opt/python27/bin/python

__author__ = 'zhuyund'

# for a partition
# 1. sample docs
# 2. gen kmeans jobs
# 3. gen inference jobs

import argparse
import os, sys
import jobWriter


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


    # sampling


    # number of clusters
    ncluster = get_ncluster(size)

    # gen clustering job
    job_dir = base_dir+"/" + shard + "/jobs/"
    job_file = open(job_dir + "/kmeans.job", 'w')
    executable = "/bos/usr0/zhuyund/partition/SplitShards/kmeans.py"
    arguments = "{0} {1} {2} {3}".format(args.partition_name, shard, ncluster, 10)
    log_file = "/tmp/zhuyund_kmeans.log"
    out_file = "/bos/usr0/zhuyund/partition/SplitShards/log/kmeans.out"
    err_file = "/bos/usr0/zhuyund/partition/SplitShards/log/kmeans.err"
    job = jobWriter.jobGenerator(executable, arguments, log_file, err_file, out_file)
    job_file.write(job)
    job_file.close()
    print "kmeans job write to: " + job_dir + "/kmeans.job"

    # gen inference job
    dv_dir = base_dir+"/"+ shard + "/docvec/"
    job_file_path = job_dir + "/inference.job"
    os.system("./genInferenceJob.py {0} {1} {2} {3} {4}".format(args.repo_dir, dv_dir, ncluster, num, job_file_path))
    print "inference job write to " + job_file_path


f.close()




