#./jobSubmitter.py cwb-s11 1 -n 3 -s 30
#./jobSubmitter.py cwb-s12 1 -n 3 -s 30
#./jobSubmitter.py cwb-s13 1 -n 3 -s 30
#./jobSubmitter.py cwb-s14 1 -n 3 -s 30
#./jobSubmitter.py cwb-s15 1 -n 3 -s 30
#./jobSubmitter.py cwb-s11 2 -n 1 -s 150
#./jobSubmitter.py cwb-s12 2 -n 1 -s 150
#./jobSubmitter.py cwb-s13 2 -n 1 -s 150
#./jobSubmitter.py cwb-s14 2 -n 1 -s 150
#./jobSubmitter.py cwb-s15 2 -n 1 -s 150
sleep 30m 
condor_run "./getDocVecFromInference.py output/cwb-19/shard  cwb-19 ../cw09catB/02-DocVectors/DV/ ../cw09catB/03-Kmeans/samplingTrial19/100Clusters-5Iters-body/inference/ -f 1"
condor_run "./getDocVecFromInference.py output/cwb-20/shard  cwb-20 ../cw09catB/02-DocVectors/DV/ ../cw09catB/03-Kmeans/samplingTrial20/100Clusters-5Iters-body/inference/ -f 1"

