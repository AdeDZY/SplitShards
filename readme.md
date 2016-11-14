To split big shards:
First, create ./output and ./log
Then...

1.1 ./prep1.py  partition_name shardmaps_dir repo_dir threshold -o 
  partition_name  run name, e.g. cw09b-s1
  shardmaps_dir   the path to the shardmap (inference results) directory
  repo_dir        indri index repo directory
  threshold       shard size threshold. Shards largers than this will be split
  -o 	--oneRepo, -o   using only one index repo. For example, cw09b has only 1 repo, but cw09a has 10 repos
 
  This step will create output/{partition_name}/ 
  output/{partition_name}/shard: list of shards that required to be split

1.2 ./getDocVecFromInference.py shard_file partition_name dat_dir infer_dir -f 0 -d cw12b 
  shard_file            output/{partition_name}/shard created by prep1.py
  partition_name        run name, e.g. cwb-11
  dat_dir               original document vector directory generated during
                        the initial clustering
  infer_dir             inference directory during the inital clustering
