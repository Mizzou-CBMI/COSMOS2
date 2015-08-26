#!/bin/bash
set -e
set -o pipefail
cd /Users/egafni/projects/Cosmos/examples/analysis_output/ex1
mkdir -p world/1

wc -c world/1/cat.txt > world/1/wc.txt