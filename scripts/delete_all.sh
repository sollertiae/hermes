#!/bin/bash
curl -s http://localhost:8000/jobs | python3 -c "
import json, sys, subprocess
jobs = json.load(sys.stdin)
for j in jobs:
    subprocess.run(['curl', '-s', '-X', 'DELETE', 'http://localhost:8000/delete',
        '-H', 'Content-Type: application/json',
        '-d', json.dumps({'uid': j['UID']})])
print(f'Deleted {len(jobs)} jobs')
"
