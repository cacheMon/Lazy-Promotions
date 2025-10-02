#!/bin/bash

ignore_obj_size=true
traces_dir=/ltdata/data/oracleReuse
task_out=~/task
output_dir=/mnt/nfs/results
distcomp=~/distComp

rm $task_out
touch $task_out


while IFS= read -r path; do
    if [ -z "$path" ] || [[ "$path" == \#* ]]; then
        continue
    fi

    file="$traces_dir/$path"

    if [ ! -f "$file" ]; then
        echo "File '$file' does not exist."
        continue
    fi

    size=$(stat --format="%s" "$file")
    gb=$(( (size + 1024*1024*1024 - 1) / (1024*1024*1024) ))
    min_dram=$(( gb/4+1 ))
    priority=$(( 100/gb + 1 ))

    for cache_size in 0.01; do
        for p in age delayfr lru fifo clock random_lru arc_prob arc_lru arc_delay arc_batch arc_fr twoq_prob twoq_lru twoq_delay twoq_batch twoq_fr; do
            mkdir -p $output_dir/$cache_size/$p
        done
        for p in 0.01 0.05 0.1 0.2 0.4 0.5 0.6 0.8 0.9; do
            echo "shell:$priority:$min_dram:1:cd $output_dir/$cache_size/prob && ~/bobCacheSim/build/bin/cachesim $file oracleGeneral lru-prob -e prob=$p $cache_size --ignore-obj-size 1" >> $task_out
        done
        for p in 0.1 0.2 0.3 0.4 0.5 0.6 0.7 0.8 0.9; do
            echo "shell:$priority:$min_dram:1:cd $output_dir/$cache_size/batch && ~/bobCacheSim/build/bin/cachesim $file oracleGeneral batch -e batch-size=$p $cache_size --ignore-obj-size 1" >> $task_out
        done
        for p in 0.1 0.2 0.3 0.4 0.5 0.6 0.7 0.8 0.9; do
            echo "shell:$priority:$min_dram:1:cd $output_dir/$cache_size/delay && ~/bobCacheSim/build/bin/cachesim $file oracleGeneral lru-delay -e delay-time=$p $cache_size --ignore-obj-size 1" >> $task_out
        done
        for p in 1 2 3 4 5 10 15 20; do
            echo "shell:$priority:$min_dram:1:cd $output_dir/$cache_size/clock && ~/bobCacheSim/build/bin/cachesim $file oracleGeneral clock -e n-bit-counter=$p $cache_size --ignore-obj-size 1" >> $task_out
        done
        for p in 1 2 4 8 16 32 64 128; do
            echo "shell:$priority:$min_dram:1:cd $output_dir/$cache_size/random_lru && ~/bobCacheSim/build/bin/cachesim $file oracleGeneral randomlru -e n-samples=$p $cache_size --ignore-obj-size 1" >> $task_out
        done
        for a in prob lru delay batch fr; do
            echo "shell:$priority:$min_dram:1:cd $output_dir/$cache_size/arc_$a && ~/bobCacheSim/build/bin/cachesim $file oracleGeneral arc-$a $cache_size --ignore-obj-size 1" >> $task_out
            echo "shell:$priority:$min_dram:1:cd $output_dir/$cache_size/twoq_$a && ~/bobCacheSim/build/bin/cachesim $file oracleGeneral twoq-$a $cache_size --ignore-obj-size 1" >> $task_out
        done
        for p in 0.01 0.05 0.1 0.2 0.4 0.5; do
            echo "shell:$priority:$min_dram:1:cd $cache_size/$cache_size/delayfr && ~/bobCacheSim/build/bin/cachesim $file oracleGeneral delayfr -e delay-ratio=$p $cache_size --ignore-obj-size 1" >> $task_out
        done
        for p in 0.16 0.2 0.25 0.3 0.4 0.5 1 1.5 2 2.5; do
            echo "shell:$priority:$min_dram:1:cd $output_dir/$cache_size/age && ~/bobCacheSim/build/bin/cachesim $file oracleGeneral age -e scaler=$p $cache_size --ignore-obj-size 1" >> $task_out
        done
        echo "shell:$priority:$min_dram:1:cd $output_dir/$cache_size/fifo && ~/bobCacheSim/build/bin/cachesim $file oracleGeneral fifo $cache_size --ignore-obj-size 1" >> $task_out
        echo "shell:$priority:$min_dram:1:cd $output_dir/$cache_size/lru && ~/bobCacheSim/build/bin/cachesim $file oracleGeneral lru $cache_size --ignore-obj-size 1" >> $task_out
    done
done < ./datasets.txt

cd $distcomp
python redisManager.py --task "loadTask" --taskfile ~/task
