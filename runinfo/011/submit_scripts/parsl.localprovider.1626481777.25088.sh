
export JOBNAME=$parsl.localprovider.1626481777.25088
set -e
export CORES=$(getconf _NPROCESSORS_ONLN)
[[ "1" == "1" ]] && echo "Found cores : $CORES"
WORKERCOUNT=1
FAILONANY=0
PIDS=""

CMD() {
process_worker_pool.py --debug --max_workers=3 -a Ananths-Macbook.local,206.189.29.228,10.31.2.198 -p 0 -c 1 -m None --poll 100 --task_port=54336 --result_port=54705 --logdir=/Users/ananthhariharan/Documents/Code/GitHub/Parsl-Project/runinfo/011/htex_Local --block_id=0 --hb_period=2  --hb_threshold=5 --cpu-affinity none 
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