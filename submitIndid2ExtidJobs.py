#!/opt/python27/bin/python
__author__ = 'zhuyund'

import argparse
import os
import time

parser = argparse.ArgumentParser()
parser.add_argument("partition_name")
args = parser.parse_args()

base_dir = "/bos/usr0/zhuyund/partition/SplitShards/output/" + args.partition_name
shard_file = open(base_dir + "/shard")
for line in shard_file:
    shard = line.strip()
    job_path = base_dir + "/" + shard + "/jobs/extid2intid.job"
    cmd = "condor_submit " + job_path
    os.system(cmd)
    time.sleep(70)
shard_file.close()
