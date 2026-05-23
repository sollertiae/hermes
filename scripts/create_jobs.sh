#!/bin/bash
N=${1:-10}
for i in $(seq 1 $N); do
  MINUTES=$((RANDOM % 59 + 1))
  curl -s -X POST http://localhost:8000/job \
    -H "Content-Type: application/json" \
    -d "{\"name\":\"stress-job-$i\",\"cron\":\"*/$MINUTES * * * *\",\"type\":\"http\",\"payload\":{\"url\":\"https://github.com/sollertiae\",\"method\":\"GET\",\"timeout\":10}}"
done
