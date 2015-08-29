#!/opt/python27/bin/python
__author__ = 'zhuyund'

import argparse
import os
import time

parser = argparse.ArgumentParser()
parser.add_argument("partition_name")
parser.add_argument("job_type", type=int, help="1:extid2intid, 2:intid2docVec 3:kmeans 4:inference, 5:shardmap")
parser.add_argument("--sleep", "-s", type=int, help="sleep time in seconds", default=90)
parser.add_argument("--nbatch", "-n", type=int, help="submit n batches at one time", default=1)
parser.add_argument("--start", "-t", type=int, help="start from this line of shard file", default=1)
parser.add_argument("--end", "-e", type=int, help="submit to this line of shard file", default=1000)
args = parser.parse_args()

base_dir = "/bos/usr0/zhuyund/partition/SplitShards/output/" + args.partition_name
shard_file = open(base_dir + "/shard")

n = 0
n_line = 0
for line in shard_file:
    shard = line.strip().split()[0]
    n_line += 1
    if n_line < args.start:
        continue
    if n_line > args.end:
        break
    if args.job_type == 1:
        job_path = base_dir + "/" + shard + "/jobs/extid2intid.job"
    elif args.job_type == 2:
        job_path = base_dir + "/" + shard + "/jobs/dumpVectors.job"
    elif args.job_type == 3:
        job_path = base_dir + "/" + shard + "/jobs/kmeans.job"
    elif args.job_type == 4:
        job_path = base_dir + "/" + shard + "/jobs/inference.job"
    elif args.job_type == 5:
        job_path = base_dir + "/" + shard + "/jobs/shardmap.job"
    else:
        print "wrong job type!"

    cmd = "condor_submit " + job_path
    os.system(cmd)
    n += 1
    if n % args.nbatch == 0:
        time.sleep(args.sleep)
shard_file.close()
