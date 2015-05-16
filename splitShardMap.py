#!/opt/python27/bin/python

__author__ = 'zhuyund'

import argparse
import os
import math

# split sharmap into several files
# for parallelization
# each file contains 100000 extids

parser = argparse.ArgumentParser()
parser.add_argument("shardmap_path")
parser.add_argument("output_dir")
args = parser.parse_args()

if not os.path.exists(args.output_dir):
    os.makedirs(args.output_dir)

sharmap_file = open(args.shardmap_path)
n_extid = 0
output_file = open("tmp.f", "w")
for extid in sharmap_file:
    if n_extid % 100000 == 0:
        output_file.close()
        output_file = open("{0}/{1}.extid".format(args.output_dir, n_extid/100000 + 1), "w")
    output_file.write(extid)
    n_extid += 1

output_file.close()

# record number of files
log_file = open("{0}/split.log".format(args.output_dir), "w")
log_file.write("n_extid {0}\n".format(n_extid))
log_file.write("n_files {0}\n".format(int(math.ceil(n_extid/1000000))))
log_file.close()







