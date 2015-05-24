#!/usr/bin/env python
import time
import subprocess
import sys

# Usage of ./ccbench:
#  -control_db_doc_size=512:
#  -control_db_name="control_db":
#  -control_db_num_docs=1000000:
#  -control_db_num_workers=10:
#  -control_db_scan_limit=1000:
#  -mongo_url="localhost":
#  -rebuid_dbs=false:
#  -run_duration=10s: run duration in seconds
#  -test_db_doc_size=512:
#  -test_db_name="test_db":
#  -test_db_num_docs=1000000:
#  -test_db_num_workers=10:
#  -test_db_scan_limit=1000:
#  -verbose=false: print stats each second

BENCH_CMD_PATH = 'cmd/ccbench/ccbench'
MONGO_URL = 'localhost'
RUN_DURATION = 10
WAIT_INTERVAL = 5

CONTROL_WORKERS = 10
CONTROL_SCAN_LIMIT = 1000
CONTROL_DOC_SIZE = 512

TEST_WORKERS_START = 0
TEST_WORKERS_END = 500
TEST_WORKERS_INCREMENT = 10
TEST_SCAN_LIMIT_START = 100
TEST_SCAN_LIMIT_END = 1000000
TEST_SCAN_LIMIT_MULTIPLIER = 2
TEST_DOC_SIZE = 512

test_workers = TEST_WORKERS_START

print("control_worker_count, control_scan_size, control_doc_size, test_worker_count, test_scan_size, test_doc_size,"
        " control_ops_sec, control_latency_op, test_ops_sec, test_latency_op")

while test_workers <= TEST_WORKERS_END:
    test_scan_limit = TEST_SCAN_LIMIT_START
    while test_scan_limit <= TEST_SCAN_LIMIT_END:
        bench_cmd = [
            BENCH_CMD_PATH,
            "-control_db_num_workers=%d" % CONTROL_WORKERS,
            "-control_db_scan_limit=%d" % CONTROL_SCAN_LIMIT,
            "-control_db_doc_size=%d" % CONTROL_DOC_SIZE,
            "-test_db_num_workers=%d" % test_workers,
            "-test_db_scan_limit=%d" % test_scan_limit,
            "-test_db_doc_size=%d" % TEST_DOC_SIZE,
            "-run_duration=%ds" % RUN_DURATION,
            "-mongo_url=%s" % MONGO_URL,
        ]
        output = subprocess.check_output(bench_cmd)
        sys.stdout.write(output)
        time.sleep(WAIT_INTERVAL)
        test_scan_limit *= TEST_SCAN_LIMIT_MULTIPLIER
    test_workers += TEST_WORKERS_INCREMENT
