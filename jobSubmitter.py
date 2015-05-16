#!/opt/python27/bin/python
__author__ = 'zhuyund'

import argparse
import os
import time

parser = argparse.ArgumentParser()
parser.add_argument("partition_name")
parser.add_argument("job_type", type=int, help="1:extid2intid, 2:intid2docVec")
parser.add_argument("--sleep", "-s", type=int, help="sleep time in seconds", default=90)
args = parser.parse_args()

base_dir = "/bos/usr0/zhuyund/partition/SplitShards/output/" + args.partition_name
shard_file = open(base_dir + "/shard")
for line in shard_file:
    shard = line.strip().split()[0]
    if args.job_type == 1:
        job_path = base_dir + "/" + shard + "/jobs/extid2intid.job"
    elif args.job_type == 2:
        job_path = base_dir + "/" + shard + "/jobs/dumpVectors.job"
    else:
        print "wrong job type!"

    cmd = "condor_submit " + job_path
    os.system(cmd)
    time.sleep(args.sleep)
shard_file.close()