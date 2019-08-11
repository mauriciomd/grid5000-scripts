#!/bin/bash

if [ $# -ne 2 ]; then
    echo "./start-spark.sh ssh_password spark_nodes.txt"
    exit
fi

spark_nodes=$(cat $2)
master=""
counter=1
for node in $spark_nodes
do
	if [ $counter -eq 1 ]; then
		master=$node
		sshpass -p $1 ssh hadoop@$node ./spark/sbin/start-master.sh -h $node
	else
		sshpass -p $1 ssh hadoop@$node ./spark/sbin/start-slave.sh spark://$master:7077
	fi
	counter=$((counter+1))
done