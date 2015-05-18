#!/opt/python27/bin/python
import string
import sys, os
import argparse

#!/opt/python27/bin/python

__author__ = 'zhuyund'

import argparse
import os
import jobWriter

parser = argparse.ArgumentParser()
parser.add_argument("dv_dir")
parser.add_argument("kmeans_dir")
parser.add_argument("n_clusters")
parser.add_argument("n_shardmap_files", type=int)
parser.add_argument("output_file_path", help="write condor jobs into here")
parser.add_argument("lamda")
parser.add_argument("--ref_threshold","-r", type=float, help="ignore terms with ref > threshold", default=1.0)
args = parser.parse_args()

executable = '/bos/tmp11/zhuyund/partition/Inference-field/inference'

log_file = "/tmp/zhuyund_infernce.log"
log_dir = "/bos/usr0/zhuyund/partition/SplitShards/log/"
err_file = log_dir + "inference.err"
out_file = log_dir + "inference.out"

if not os.path.exists(log_dir):
    os.makedirs(log_dir)


if not os.path.exists(args.kmeans_dir + "/inference"):
    os.makedirs(args.kmeans_dir + "/inference")

job_file = open(args.output_file_path, "w")

for i in range(1, args.n_shardmap_files + 1):

    arguments = "{0}/{1}.dat  {2}/centroids/ {2}/inference/{1}.inference {3} {5}1 1 field {4} ".format(args.dv_dir,
                                                                                i,
                                                                                args.kmeans_dir,
                                                                                args.n_clusters, args.ref_threshold, args.lamda)

    job = jobWriter.jobGenerator(executable, arguments, log_file, err_file, out_file)

    job_file.write(job)

job_file.close()





