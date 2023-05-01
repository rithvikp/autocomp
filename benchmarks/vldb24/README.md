# VLDB '24 Submission

This directory contains the benchmark scripts used in our VLDB'24 submission. For our protocol implementations, refer to the top-level README.

Each `*_benchmark.py` script defines a single benchmark (which may include hyperparameter searches and multiple trials). The plotting code and results are in [plots/](plots/).

The benchmarks automatically provision cloud machines as necessary, using the [Hydro CLI](https://github.com/hydro-project/hydroflow/tree/main/hydro_cli).