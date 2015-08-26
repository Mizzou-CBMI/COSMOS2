#!/bin/bash
set -e
set -o pipefail
cd /Users/egafni/projects/Cosmos/examples/analysis_output/ex1
mkdir -p hello/1

wc -c hello/1/cat.txt > hello/1/wc.txt