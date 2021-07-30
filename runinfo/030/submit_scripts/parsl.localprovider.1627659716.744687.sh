
export JOBNAME=$parsl.localprovider.1627659716.744687
set -e
export CORES=$(getconf _NPROCESSORS_ONLN)
[[ "1" == "1" ]] && echo "Found cores : $CORES"
WORKERCOUNT=1
FAILONANY=0
PIDS=""

CMD() {
process_worker_pool.py --debug  -a 138.68.145.31,10.31.2.79,Ananths-Macbook.local -p 0 -c 0.25 -m None --poll 100 --task_port=54044 --result_port=54120 --logdir=/Users/ananthhariharan/Documents/Code/GitHub/Parsl-Project/runinfo/030/htex_Local --block_id=1 --hb_period=2  --hb_threshold=5 --cpu-affinity none 
}
for COUNT in $(seq 1 1 $WORKERCOUNT); do
    [[ "1" == "1" ]] && echo "Launching worker: $COUNT"
    CMD $COUNT &
    PIDS="$PIDS $!"
done

ALLFAILED=1
ANYFAILED=0
for PID in $PIDS ; do
    wait $PID
    if [ "$?" != "0" ]; then
        ANYFAILED=1
    else
        ALLFAILED=0
    fi
done

[[ "1" == "1" ]] && echo "All workers done"
if [ "$FAILONANY" == "1" ]; then
    exit $ANYFAILED
else
    exit $ALLFAILED
fi
