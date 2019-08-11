#!/bin/bash

if [ $# -ne 2 ]; then
        echo "./copy-file.sh spark-folder nodes-file"
        exit
fi

# Copy the Spark-Env file to new spark distribution 
cp spark-env.sh $1/conf/

spark_nodes=$(cat $2)
for node in $spark_nodes
do
    ssh root@$node rm -rf /home/hadoop/spark/
    scp -r $1 hadoop@$node:~/spark
done

./start-spark.