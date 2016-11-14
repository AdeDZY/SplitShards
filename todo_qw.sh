#condor_run "./getDocVecFromInference.py output/gov2-qw160-aol-df200-s1/shard gov2-qw160-aol-df200-s1 ../gov2/02-DocVectors/DV/ ../gov2/03-Kmeans/samplingTrial1_qweight/150Clusters-10Iters-qw160-aol-df200/inference/ -d gov2 -f 0"
#condor_run "./getDocVecFromInference.py output/gov2-qw320-aol-df200-s1/shard gov2-qw320-aol-df200-s1 ../gov2/02-DocVectors/DV/ ../gov2/03-Kmeans/samplingTrial1_qweight/150Clusters-10Iters-qw320-aol-df200/inference/ -d gov2 -f 0"
for i in {6..10}
do
	condor_run "./getDocVecFromInference.py output/gov2-qw80-cent1-s${i}/shard gov2-qw80-cent1-s${i} ../gov2/02-DocVectors/DV/ ../gov2/03-Kmeans/samplingTrial${i}_qweight/149Clusters-10Iters-qw80-cent1/inference/ -d gov2 -f 0"
done
