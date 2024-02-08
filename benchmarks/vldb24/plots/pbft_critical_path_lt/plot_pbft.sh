#!/bin/bash
python pbft_plot.py --results results.csv auto1.csv auto3.csv auto5.csv --titles BasePBFT "ScalablePBFT, 1 partition" "ScalablePBFT, 3 partitions" "ScalablePBFT, 5 partitions"