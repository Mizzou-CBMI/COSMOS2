#!/bin/bash
set -e
set -o pipefail
cd /Users/egafni/projects/Cosmos/examples/analysis_output/ex1
mkdir -p hello/2

wc -c hello/2/cat.txt > hello/2/wc.txt