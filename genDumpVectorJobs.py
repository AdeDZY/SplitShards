#!/opt/python27/bin/python

__author__ = 'zhuyund'

import argparse
import os
import jobWriter

parser = argparse.ArgumentParser()
parser.add_argument("repo_dir", help="inri repo dir")
parser.add_argument("intid_dir", help="intid files")
parser.add_argument("dv_dir", help="write doc vectors here")
parser.add_argument("n_intid_files", type=int)
parser.add_argument("output_file_path", help="write condor jobs into here")
args = parser.parse_args()

executable = "/bos/usr0/zhuyund/partition/DocVectors/dumpVectors"

log_file = "/tmp/zhuyund_dumpVectors.log"
log_dir = "/bos/usr0/zhuyund/partition/SplitShards/log/"
err_file = log_dir + "dumpVectors.err"
out_file = log_dir + "dumpVectors.out"

if not os.path.exists(log_dir):
    os.makedirs(log_dir)

if not os.path.exists(args.dv_dir):
    os.makedirs(args.dv_dir)

job_file = open(args.output_file_path, "w")

#  std::string repDir = argv[1]; // parent dir of repos
#  std::string docidFile = argv[2];
#  std::string outFileName = argv[3];
#  std::string stoplistFile = argv[4];

for i in range(1, args.n_intid_files + 1):
    arguments = "{0} {1}/{2}.intid {3}/{2}.dat /bos/usr0/zhuyund/partition/DocVectors/stoplist.dft".format(args.repo_dir, args.intid_dir, i, args.dv_dir)

    job = jobWriter.jobGenerator(executable, arguments, log_file, err_file, out_file)

    job_file.write(job)

job_file.close()















