#!/bin/bash
set -e
set -o pipefail
cd /Users/egafni/projects/Cosmos/examples/analysis_output/ex1

cat hello/1/wc.txt hello/2/wc.txt world/1/wc.txt world/2/wc.txt > cat.txt