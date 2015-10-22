#!/opt/python27/bin/python

__author__ = 'zhuyund'

# for a partition
# 1. sample docs
# 2. gen kmeans jobs
# 3. gen inference jobs

import argparse
import os, sys
import jobWriter


def get_ncluster(shard_size, aim):
    """
    number of clusters
    :param sahrd_size:
    :return: int
    """

    n_cluster = 2

    if shard_size < aim:
        return 1

    while True:
        if float(shard_size) / n_cluster - aim < aim - float(shard_size) / (n_cluster + 1):
            break
        n_cluster += 1
    return n_cluster


parser = argparse.ArgumentParser()
parser.add_argument("partition_name")
parser.add_argument("qw")
parser.add_argument("--shard", "-s", help="only one shard", default="")
parser.add_argument("--start", "-t", type=int, default=1)
parser.add_argument("--end", "-e", type=int, default=1000)
parser.add_argument("--ref_threshold", "-r", type=float, default=1.0)
parser.add_argument("--constant", "-c", type=int, default=0, help="1 = use constant weight")
args = parser.parse_args()

base_dir = "/bos/usr0/zhuyund/partition/SplitShards/output/" + args.partition_name
print base_dir

f = open(base_dir + "/shard")  # splitted shard ids and numbers
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


    # number of clusters
    stream = os.popen("tail -n1 {0}/{1}/kmeans/log".format(base_dir, shard))
    ncluster = int(stream.readline().split(':')[1])
    print ncluster


    # gen inference job
    job_dir = base_dir + "/" + shard + "/jobs/"
    dv_dir = base_dir + "/" + shard + "/docvec/"
    job_file_path = job_dir + "/inference.job"
    os.system("./genInferenceJob_qw.py {0} {1} {2} {3} {4} {6} -r {5} -c {7}".format(dv_dir,
                                                                                     base_dir + "/" + shard + "/kmeans/",
                                                                                     ncluster,
                                                                                     num,
                                                                                     job_file_path,
                                                                                     args.ref_threshold,
                                                                                     args.qw,
                                                                                     args.constant))
    print "inference job write to " + job_file_path

    # gen getShardMap job
    shardmap_dir = base_dir + "/" + shard + "/shardMap/"
    infer_dir = base_dir + "/" + shard + "/kmeans/inference/"
    extid_dir = base_dir + "/" + shard + "/extid/"

    job_file_path = job_dir + "/shardmap.job"
    executable = "/bos/usr0/zhuyund/partition/SplitShards/getShardMap.sh"
    arguments = "{0} {1} {2} {3} {4}".format(infer_dir, extid_dir, ncluster, shardmap_dir, num)
    log_file = "/tmp/zhuyund_shardmap.log"
    out_file = "/bos/usr0/zhuyund/partition/SplitShards/log/shardmap.out"
    err_file = "/bos/usr0/zhuyund/partition/SplitShards/log/shardmap.err"
    job = jobWriter.jobGenerator(executable, arguments, log_file, err_file, out_file)
    job_file = open(job_file_path, 'w')
    job_file.write(job)
    job_file.close()
    print "shardmap job write to: " + job_dir + "/shardmap.job"

f.close()
