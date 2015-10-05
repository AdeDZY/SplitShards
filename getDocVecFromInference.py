__author__ = 'zhuyund'

import argparse
import os, sys

parser = argparse.ArgumentParser()
parser.add_argument("shard_file")
parser.add_argument("extid_dir")
parser.add_argument("dat_dir")
parser.add_argument("infer_dir")
parser.add_argument("--dataset", "-d", default='cwb', help="gov2, cwa, cwb")
args = parser.parse_args()

if args.dataset != "cwb":
    print "Sorry! Do not have function for " + args.dataset
    exit(-1)

shards = set()
sizes = {}
shard_dirs = {}
out_files = {}
output_lines = {}

n_docs = 50220423
split_size = 100000
pref = "cw09catB"

for line in open(args.shard_file):
    shard, n_splits, size = line.split()
    shard = int(shard)
    shards.add(shard)
    shard_dirs[shard] = args.extid_dir + '/' + str(shard) + '/'
    if not os.path.exists(shard_dirs[shard]):
        os.makedirs(shard_dirs[shard])
    sizes[shard] = 0
    out_files[shard] = open(shard_dirs[shard] + str(sizes/split_size + 1), 'w')
    output_lines[shard] = ""


start = 1
while start <= n_docs:
    end = start + split_size - 1
    if end > n_docs:
        end = n_docs
    infer_file = open("{0}/{1}_{2}-{3}.inference".format(args.infer_dir, pref, start, end))
    dat_file = open("{0}/{1}_{2}-{3}.dat".format(args.dat_dir, pref, start, end))
    for infer_line in infer_file:
        dat_line = dat_file.readline().strip()
        if not dat_line:
            infer_file.close()
            dat_file.close()
            continue
        intid, shard = [int(t) for t in infer_line.split(':')]
        output_lines[shard] += dat_line + '\n'
        sizes[shard] += 1

        if sizes[shard] % 1000 == 0:
            if sizes[shard] % 100000 == 0:
                out_files[shard].close()
                out_files[shard] = open(shard_dirs[shard] + str(sizes/split_size + 1), 'w')
            out_files[shard].write(output_lines[shard])
            output_lines[shard] = ""
    start = end + 1

for shard in shards:
    if output_lines[shard]:
        out_files[shard].write(output_lines[shard])
        out_files[shard].close()



