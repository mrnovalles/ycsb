#!/bin/bash
# Script to run some experiments on a naive 
# impl of YCSB-S4 binding
source ~/.bashrc
output="/home/valles/programming/YCSB/output-cp"
bin="/home/valles/programming/YCSB/bin"
workloads="/home/valles/programming/YCSB/workloads"
params="/home/valles/programming/YCSB"
#Test if output dir exists, if not create
if [ ! -d $output ]; then
	mkdir $output
fi
if [ -z $1 ] 
then
	echo "Usage: `basename $0` [load| run | shell] numThreads {numOpsPerSec}"
	exit 65
fi
if [ ! -n $2 ]; then
	threads=10 #defaults to 10 threads
else
	threads=$2
fi
d=`date +%H%M%S%N`
CMD="$bin/ycsb $1 s4 -P $workloads/workloadinsert -P $params/large.dat"
for pe in 1 10 100
do
	s4cluster &
	sleep 4
	s4adapter &
	sleep 4

	$CMD -threads 1 -target 1500 -p s4.npes=$pe -s > $output/numPEs$pe-$d
	
	a=`cat /home/valles/programming/incubator-s4/build/s4-image/s4-core/logs/s4-core/*.log | grep nanos | awk {'print $7'}| wc -l`
		
	if [ "$a" -eq 10000 ]; then
		echo "All events checkpointed"

	fi
	
	cat /home/valles/programming/incubator-s4/build/s4-image/s4-core/logs/s4-core/*.log | grep nanos | awk {'print $7'} > /home/valles/results/$pe.redis
	cleans4
	
	for i in `ps aux | grep s4-core| grep -v 'eclipse|grep'| awk {'print $2'}`
	do
		kill -9 $i
	done
done
