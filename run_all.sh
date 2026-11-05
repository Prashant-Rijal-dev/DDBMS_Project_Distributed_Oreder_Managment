#!/usr/bin/env bash
# Runs every scenario and saves the evidence for your report.
cd "$(dirname "$0")"
mkdir -p results
python3 demo.py all 2>&1 | tee results/demo_output.txt
echo; echo "Saved to results/demo_output.txt"
