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
parser.add_argument("--shard","-s",  help="only one shard", default="")
parser.add_argument("--start","-t",  type=int, default=1)
parser.add_argument("--end","-e",  type=int, default=200)
parser.add_argument("--ref_threshold","-r",  type=float, default=1.0)
args = parser.parse_args()

base_dir = "/bos/usr0/zhuyund/partition/SplitShards/output/" + args.partition_name
print base_dir

f = open(base_dir + "/shard") # splitted shard ids and numbers
sample_rate = 0.1  # sample rate = 10%
nline = 0
for line in f:
    nline += 1
    if nline < args.start or nline > args.end:
        continue
    line = line.strip()
    shard, num, size = line.split()
    if args.shard and shard != args.shard:
        continue
    num = int(num)
    size = int(size)
    if shard == "total" or shard == "size":
        continue


    # sampling
   # cmd = "./sampleDoc.py {0} {1} {2} {3}".format(args.partition_name, shard, num, sample_rate)
   # os.system(cmd)

    # number of clusters
    ncluster = get_ncluster(size)

    # gen clustering job
    job_dir = base_dir+"/" + shard + "/jobs/"
    job_file = open(job_dir + "/kmeans.job", 'w')
    executable = "/bos/usr0/zhuyund/partition/SplitShards/kmeans.py"
    arguments = "{0} {1} {2} {3} -r {4}".format(args.partition_name, shard, ncluster, 10, args.ref_threshold)
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
    os.system("./genInferenceJob.py {0} {1} {2} {3} {4} -r {5}".format(dv_dir, base_dir+"/"+shard+"/kmeans/", ncluster, num, job_file_path, args.ref_threshold))
    print "inference job write to " + job_file_path


f.close()




