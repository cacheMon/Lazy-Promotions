#!/bin/bash

set -eux

simulator=$HOME/Lazy-Promotions/simulator-concurrent/_build/bin

run_simulation()
{
    local algorithm=$1
    local param_name=$2
    local param_val=$3
    local cache_size=$4
    local num_threads=$5

    if [[ $algorithm == "fifo" || $algorithm == "lru" ]]; then
        $simulator/cachesim $algorithm $cache_size --num-thread $num_threads
    else
        $simulator/cachesim $algorithm -e $param_name=$param_val $cache_size --num-thread $num_threads
    fi
}

num_iteration=5
output_dir=~/results/
mkdir -p $output_dir

threads_list=(16)

for iteration in $(seq 1 $num_iteration); do

    algo="fifo"
    mkdir -p $output_dir/$algo
    cache_sizes=("94000" "74000" "50000" "24000")
    for size in "${cache_sizes[@]}"; do
        for threads in "${threads_list[@]}"; do
            run_simulation $algo "" "" $size $threads >> $output_dir/$algo/result_${iteration}.txt
        done
    done

    algo="lru"
    mkdir -p $output_dir/$algo
    cache_sizes=("91000" "65000" "42000" "18000")
    for size in "${cache_sizes[@]}"; do
        for threads in "${threads_list[@]}"; do
            run_simulation $algo "" "" $size $threads >> $output_dir/$algo/result_${iteration}.txt
        done
    done

    algo="lru-prob"
    mkdir -p $output_dir/$algo
    cache_sizes=("88000" "68000" "48000" "20000")
    for p in 0.9 0.8 0.6 0.5 0.4 0.2 0.1 0.05 0.01; do
        for size in "${cache_sizes[@]}"; do
            for threads in "${threads_list[@]}"; do
                run_simulation $algo "prob" $p $size $threads >> $output_dir/$algo/result_${iteration}.txt
            done
        done
    done

    algo="lru-delay"
    mkdir -p $output_dir/$algo
    cache_sizes=("90000" "68000" "46000" "19000")
    for p in 0.1 0.2 0.3 0.4 0.5 0.6 0.7 0.8 0.9 ; do
        for size in "${cache_sizes[@]}"; do
            for threads in "${threads_list[@]}"; do
                run_simulation $algo "delay-time" $p $size $threads >> $output_dir/$algo/result_${iteration}.txt
            done
        done
    done

    algo="clock"
    mkdir -p $output_dir/$algo
    cache_sizes=("90000" "63000" "42000" "17000")
    for p in 1 2 3 4 5 10 15 20 ; do
        for size in "${cache_sizes[@]}"; do
            for threads in "${threads_list[@]}"; do
                run_simulation $algo "n-bit-counter" $p $size $threads >> $output_dir/$algo/result_${iteration}.txt
            done
        done
    done

    algo="batch"
    mkdir -p $output_dir/$algo
    cache_sizes=("90000" "63000" "41000" "17000")
    for p in 0.1 0.2 0.3 0.4 0.5 0.6 0.7 0.8 0.9 ; do
        for size in "${cache_sizes[@]}"; do
            for threads in "${threads_list[@]}"; do
                run_simulation $algo "batch-size" $p $size $threads >> $output_dir/$algo/result_${iteration}.txt
            done
        done
    done

    algo="randomK"
    mkdir -p $output_dir/$algo
    cache_sizes=("92000" "69000" "47000" "20000")
    for p in 1 2 4 8 16 32 64 128; do
        for size in "${cache_sizes[@]}"; do
            for threads in "${threads_list[@]}"; do
                run_simulation $algo "k" $p $size $threads >> $output_dir/$algo/result_${iteration}.txt
            done
        done
    done
done






