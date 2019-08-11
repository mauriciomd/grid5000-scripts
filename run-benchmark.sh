#!/bin/bash

size=""
algorithm=""

while [ "$#" -gt 0 ]; do
    case "$1" in
        -a) algorithm="$2"; shift 2;;
        -s) size="$2"; shift 2;;
        --algorithm=*) algorithm="${1#*=}"; shift 1;;
        --size=*) size="${1#*=}"; shift 1;;
        -*) echo "unknown option: $1" >&2; exit 1;;
    esac
done

prepare() {
    echo "Copying the Spark Conf file"
    cp conf/spark.conf .

    echo "Preparing the data."
    echo "Generating Pagerank dataset..."
    ./bin/workloads/websearch/pagerank/prepare/prepare.sh

    echo "Generating K-Means dataset"
    ./bin/workloads/ml/kmeans/prepare/prepare.sh

    echo "Generating Logistic Regression dataset"
    ./bin/workloads/ml/lr/prepare/prepare.sh

    echo -e "Done!\n"
}


run_pagerank() {
    echo "Running Pagerank!"
    for i in $(seq 1 20)
    do
        echo "Execution $i"
        ./bin/workloads/websearch/pagerank/spark/run.sh
    done
}


run_kmeans() {
    echo "Running K-Means!"
    for i in $(seq 1 20)
    do
        echo "Execution $i"
        ./bin/workloads/ml/kmeans/spark/run.sh
    done
}


run_lr() {
    echo "Running Logistic Regression!"
    for i in $(seq 1 20)
    do
        echo "Execution $i"
        ./bin/workloads/ml/lr/spark/run.sh
    done
}

run_all() {
    echo "Memory avaliable: $3"
    sed "s/4g/$3/" spark.conf > spark.txt
    rm -rf conf/spark.conf
    mv spark.txt conf/spark.conf
    run_pagerank
    run_kmeans
    run_lr
    mv report/hibench.report $1-$2-$3.report
    echo -e "Done!\n"    
}

prepare
run_all $algorithm $size "4g"
run_all $algorithm $size "3g"
run_all $algorithm $size "3g"
run_all $algorithm $size "2g"
run_all $algorithm $size "1536m"
run_all $algorithm $size "1g"