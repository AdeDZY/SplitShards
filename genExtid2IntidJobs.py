#!/opt/python27/bin/python

__author__ = 'zhuyund'

import argparse
import os
import math
import jobWriter

parser = argparse.ArgumentParser()
parser.add_argument("repo_dir", help="inri repo dir")
parser.add_argument("shardmap_split_dir", help="where splitted shardmap files locate")
parser.add_argument("intid_dir", help="write intid to here")
parser.add_argument("n_shardmap_files", type=int)
parser.add_argument("output_file_path", help="write condor jobs into here")
args = parser.parse_args()

executable = "/bos/usr0/zhuyund/partition/DocVectors/extid2intid"

log_file = "/tmp/zhuyund_extid2intid.log"
log_dir = "/bos/usr0/zhuyund/partition/SplitShards/log/"
err_file = log_dir + "extid2intid.err"
out_file = log_dir + "extid2intid.out"

if not os.path.exists(log_dir):
    os.makedirs(log_dir)

if not os.path.exists(args.intid_dir):
    os.makedirs(args.intid_dir)

job_file = open(args.output_file_path, "w")
for i in range(1, args.n_shardmap_files + 1):
    arguments = "{0} {1}/{2}.extid {3}/{2}.intid".format(args.repo_dir, args.shardmap_split_dir, i, args.intid_dir)

    job = jobWriter.jobGenerator(executable, arguments, log_file, err_file, out_file)

    job_file.write(job)

job_file.close()















