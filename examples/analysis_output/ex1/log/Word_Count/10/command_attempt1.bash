#!/bin/bash
set -e
set -o pipefail
cd /Users/egafni/projects/Cosmos/examples/analysis_output/ex1
mkdir -p world/2

wc -c world/2/cat.txt > world/2/wc.txt