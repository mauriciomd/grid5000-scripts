#!/bin/bash

# Default number of Hadoop nodes
N_HADOOP_NODES=2

cluster_nodes=$(cat $OAR_NODEFILE | uniq)
echo -e $cluster_nodes
kadeploy3 -f $OAR_NODEFILE -a debian8-spark.env -k

rm -rf hadoop_nodes
rm -rf spark_nodes
touch hadoop_nodes
touch spark_nodes

counter=1
for node in $cluster_nodes
do
	if [ $counter -le $N_HADOOP_NODES ]; then
		echo $node >> hadoop_nodes
	else
		echo $node >> spark_nodes
	fi
	counter=$((counter+1))
done
python3 generate_hadoop_files.py hadoop_nodes

hadoop=$(cat hadoop_nodes)
for h in $hadoop
do
	scp yarn-site.xml root@$h:/home/hadoop/hadoop/etc/hadoop/
	scp mapred-site.xml root@$h:/home/hadoop/hadoop/etc/hadoop/
    scp hdfs-site.xml root@$h:/home/hadoop/hadoop/etc/hadoop/
	scp core-site.xml root@$h:/home/hadoop/hadoop/etc/hadoop/
	scp slaves root@$h:/home/hadoop/hadoop/etc/hadoop/
done