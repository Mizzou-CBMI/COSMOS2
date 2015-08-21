#!/bin/bash
set -e
set -o pipefail
cd /Users/egafni/projects/Cosmos/examples/analysis_output/ex1
mkdir -p world/2

cat world/echo.txt > world/2/cat.txt