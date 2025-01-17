## [Optimizing Distributed Protocols with Query Rewrites](https://dl.acm.org/doi/10.1145/3639257)
David C. Y. Chu, Rithvik Panchapakesan, Shadaj Laddad, Lucky Katahanas, Chris Liu, Kaushik Shivakumar, Natacha Crooks, Joseph M. Hellerstein, Heidi Howard  
_SIGMOD '24_  
[`Technical Report`](https://arxiv.org/abs/2404.01593)

## [Bigger, not Badder: Safely Scaling Byzantine Protocols](https://dl.acm.org/doi/10.1145/3642976.3653033)
David C. Y. Chu, Chris Liu, Natacha Crooks, Joseph M. Hellerstein, Heidi Howard  
_PaPoC '24_  

---
This repository contains the implementations of various state machine replication protocols in both Scala and Dedalus. It was initially forked from [frankenpaxos](https://github.com/mwhittaker/frankenpaxos) by Michael Whittaker.

Refer to the following on how to use this codebase:
- [Setup](SETUP.md)
- [Protocols](PROTOCOLS.md)
- [It doesn't work! (FAQ)](FAQ.md)

Although most of the example benchmark scripts in this repository are located in `benchmarks/vldb24`, they are not for the VLDB '24 submission. The folder was named after our first (rejected) submission and has been modified since.

# SIGMOD '24 Artifact Evaluation
Follow instructions in [Setup](SETUP.md).  
Then, modify the cluster scripts, as seen in [Protocols](PROTOCOLS.md):  
`<project>`: Your GCP project name.  
`<zone>`: The zone where your VMs and networks are located. For example: `us-central1-a`.  
`<username>`: Your username on the VMs.

```bash
cd ~/autocomp/benchmarks/vldb24/clusters
python modify_clusters.py --cloud gcp --project <project> --zone <zone> --user <username>
```

> [!NOTE]  
> If you run into any issues, try rerunning the script. They sometimes timeout on initiation. If that doesn't work, [stop any experiments](PROTOCOLS.md#stopping-the-experiments), stop all the VMs, and contact David.


## Reproducing Figure 7
Every benchmark is run over 6+ configurations, where each run takes ~3 minutes, so be prepared to spend many hours (or days!) running the benchmarks.  

The benchmarks in the paper were each run 3 times in order to extract the median and standard deviation. This results in a 3x evaluation time, so it is currently disabled. If you wish to re-enable it, you can do so by modifying the corresponding `*_benchmark.py` file in the `benchmarks/vldb24` directory for each benchmark, and un-commenting `] #*3` at the end of `def inputs()`.

Most runs did not have large standard deviations so you should see graphs similar to what we have in the paper even with only a single run.

### <img src="https://upload.wikimedia.org/wikipedia/commons/d/d5/Rust_programming_language_black_logo.svg" width="15"/> BaseVoting
Execute the following:
```bash
cd ~/autocomp
benchmarks/vldb24/dedalus_voting_run.sh
```

#### Troubleshooting
Once the deployment completes, you should see some output every 3 minutes. If not, something has gone wrong, usually a timeout. If the benchmark fails half-way, then you can either rerun the entire benchmark, or rerun from the point of failure, by going to [benchmarks/vldb24/dedalus_voting_benchmark.py](benchmarks/vldb24/dedalus_voting_benchmark.py) and editing this section of code:
```python
for (num_client_procs, num_clients_per_proc) in [
    (1, 1),
    (1, 50),
    (1, 100),
    (2, 100),
    (3, 100),
    (4, 100),
    (5, 100),
    (6, 100),
    #(7, 100),
    #(8, 100),
    #(9, 100),
    #(10, 100),
]
```
You can identify where the benchmark stopped by checking the last output of the benchmark, which should contain something like this:
```bash
[002/024; 8.333%] 0:02:01 / 0:04:47 + 0:52:42? {'value_size': UniformReadWriteWorkload(num_keys=1, read_fraction=0.0, write_size_mean=16, write_size_std=0, name='UniformReadWriteWorkload'), 'num_client_procs': 1, 'num_clients_per_proc': 50, 'num_replicas': 3, 'leader_flush_every_n': 1, 'latency.median_ms': 0.760959, 'start_throughput_1s.p90': 58050.0}
```
This line `'num_client_procs': 1, 'num_clients_per_proc': 50` indicates that the benchmark last ran the `(1, 50)` configuration, so you should comment out `(1, 1),` and `(1, 50),` and rerun the benchmark from there.

If you run into a problem with any other benchmark, just modify the correpsonding `*_benchmark.py` in a similar way.

#### Interpreting the output
Once the script is done running, you can find the results in `/mnt/nfs/tmp`. The folder's name is based on when you ran the experiment, and which experiment you ran.
For example, my folder was named `2024-08-20_17:51:33.135629_DGDHZHGVWL_voting_lt_dedalus`.
Inside the folder, you should see folders named `001`, `002`, etc, each containing the output of an individual run.
The aggregated results over all runs is in `results.csv`. 
We wil copy this file to `plots/voting_lt`:
```bash
cp results.csv ~/autocomp/benchmarks/vldb24/plots/voting_lt/3base.csv
```

### <img src="https://upload.wikimedia.org/wikipedia/commons/d/d5/Rust_programming_language_black_logo.svg" width="15"/> ScalableVoting
Execute the following:
```bash
cd ~/autocomp
benchmarks/vldb24/dedalus_autovoting_run.sh 1
benchmarks/vldb24/dedalus_autovoting_run.sh 3
benchmarks/vldb24/dedalus_autovoting_run.sh 5
```

Once that's all done, copy `results.csv` from each run (as seen above) into [benchmarks/vldb24/plots/voting_lt](benchmarks/vldb24/plots/voting_lt), and replace `3scale1.csv`, `3scale3.csv`, and `3scale5.csv` respectively, based on the run configuration.

#### Reproducing Figure 7.a
You can now generate the plot for Figure 7.a by running the following:
```bash
cd ~/autocomp/benchmarks/vldb24/plots
python plot.py --results voting_lt/3base.csv voting_lt/3scale1.csv voting_lt/3scale3.csv voting_lt/3scale5.csv --titles "Base" "1 partition" "3 partitions" "5 partitions" --output eval-voting-lt.pdf
```
The output graph will be in `eval-voting-lt.pdf`.

### <img src="https://upload.wikimedia.org/wikipedia/commons/d/d5/Rust_programming_language_black_logo.svg" width="15"/> Base2PC
Execute the following:
```bash
cd ~/autocomp
benchmarks/vldb24/dedalus_twopc_run.sh
```
Copy `results.csv` into [benchmarks/vldb24/plots/twopc_lt](benchmarks/vldb24/plots/twopc_lt), and replace `fixed_base.csv`.

### <img src="https://upload.wikimedia.org/wikipedia/commons/d/d5/Rust_programming_language_black_logo.svg" width="15"/> Scalable2PC
Execute the following:
```bash
cd ~/autocomp
benchmarks/vldb24/dedalus_autotwopc_run.sh 1
benchmarks/vldb24/dedalus_autotwopc_run.sh 3
benchmarks/vldb24/dedalus_autotwopc_run.sh 5
```
Copy `results.csv` into [benchmarks/vldb24/plots/twopc_lt](benchmarks/vldb24/plots/twopc_lt), and replace `fixed_auto1.csv`, `fixed_auto3.csv`, and `fixed_auto5.csv` respectively, based on the run configuration.

#### Reproducing Figure 7.b
You can now generate the plot for Figure 7.b by running the following:
```bash
cd ~/autocomp/benchmarks/vldb24/plots
python plot.py --results twopc_lt/fixed_base.csv twopc_lt/fixed_auto1.csv twopc_lt/fixed_auto3.csv twopc_lt/fixed_auto5.csv --titles "Base" "1 partition" "3 partitions" "5 partitions" --output eval-2pc-lt.pdf
```
The output graph will be in `eval-2pc-lt.pdf`.

### <img src="https://upload.wikimedia.org/wikipedia/commons/d/d5/Rust_programming_language_black_logo.svg" width="15"/> BasePaxos
Execute the following:
```bash
cd ~/autocomp
benchmarks/vldb24/dedalus_multipaxos_run.sh
```
Copy `results.csv` into [benchmarks/vldb24/plots/multipaxos_lt](benchmarks/vldb24/plots/multipaxos_lt), and replace `base.csv`.

### <img src="https://upload.wikimedia.org/wikipedia/commons/d/d5/Rust_programming_language_black_logo.svg" width="15"/> ScalablePaxos
Execute the following:
```bash
cd ~/autocomp
benchmarks/vldb24/dedalus_automultipaxos_run.sh 1
benchmarks/vldb24/dedalus_automultipaxos_run.sh 3
benchmarks/vldb24/dedalus_automultipaxos_run.sh 5
```
Copy `results.csv` into [benchmarks/vldb24/plots/multipaxos_lt](benchmarks/vldb24/plots/multipaxos_lt), and replace `scale1.csv`, `scale3.csv`, and `scale5.csv` respectively, based on the run configuration.

#### Reproducing Figure 7.c
You can now generate the plot for Figure 7.c by running the following:
```bash
cd ~/autocomp/benchmarks/vldb24/plots
python plot.py --results multipaxos_lt/base.csv multipaxos_lt/scale1.csv multipaxos_lt/scale3.csv multipaxos_lt/scale5.csv --titles "Base" "1 partition" "3 partitions" "5 partitions" --output eval-paxos-lt.pdf
```
The output graph will be in `eval-paxos-lt.pdf`.

## Reproducing Figure 9
We already have the results for <img src="https://upload.wikimedia.org/wikipedia/commons/d/d5/Rust_programming_language_black_logo.svg" width="15"/> BasePaxos above.

We need to get results for <img src="https://upload.wikimedia.org/wikipedia/commons/d/d5/Rust_programming_language_black_logo.svg" width="15"/> ScalablePaxos with a slightly different configuration in order to restrict the number of machines it can use.  
Execute the following:
```bash
cd ~/autocomp
benchmarks/vldb24/dedalus_automultipaxos_restricted_run.sh
```
Copy `results.csv` into [benchmarks/vldb24/plots/multipaxos_lt](benchmarks/vldb24/plots/multipaxos_lt), and replace `11nodes.csv`.

Now we need results for BasePaxos.  
Execute the following:
```bash
cd ~/autocomp
benchmarks/vldb24/scala_paxos_run.sh
```
Copy `results.csv` into [benchmarks/vldb24/plots/multipaxos_lt](benchmarks/vldb24/plots/multipaxos_lt), and replace `base_michael.csv`.

Now we need results for CompPaxos.  
Execute the following:
```bash
cd ~/autocomp
benchmarks/vldb24/scala_comp_paxos_run.sh
```
Copy `results.csv` into [benchmarks/vldb24/plots/multipaxos_lt](benchmarks/vldb24/plots/multipaxos_lt), and replace `scale_michael.csv`.

Finally, we need results for <img src="https://upload.wikimedia.org/wikipedia/commons/d/d5/Rust_programming_language_black_logo.svg" width="15"/> CompPaxos.  
Execute the following:
```bash
cd ~/autocomp
benchmarks/vldb24/dedalus_comppaxos_run.sh
```
Copy `results.csv` into [benchmarks/vldb24/plots/multipaxos_lt](benchmarks/vldb24/plots/multipaxos_lt), and replace `dedalus_comp.csv`.

We can now generate Figure 9 by executing the following:
```bash
cd ~/autocomp/benchmarks/vldb24/plots
python plot_michael_comp.py --results multipaxos_lt/base.csv multipaxos_lt/11nodes.csv multipaxos_lt/base_michael.csv multipaxos_lt/scale_michael.csv multipaxos_lt/dedalus_comp.csv  --titles "RBasePaxos" "RScalablePaxos" "BasePaxos" "ScalablePaxos" "RCompPaxos" --output eval-michael-comp.pdf
```
The output graph will be in `eval-michael-comp.pdf`.

## Reproducing Figure 10
Execute the following:
```bash
cd ~/autocomp
benchmarks/vldb24/dedalus_microbenchmarks_run.sh
```
Copy `results.csv` into [benchmarks/vldb24/plots/microbenchmarks](benchmarks/vldb24/plots/microbenchmarks), and replace `combined.csv`.

We can now generate Figure 10 by executing the following:
```bash
cd ~/autocomp/benchmarks/vldb24/plots/microbenchmarks
python plot_bar.py
```
The output graph will be in `microbenchmarks.pdf`.
