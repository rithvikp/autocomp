# Protocols
All protocols used in the SIGMOD '24 and PaPoC '24 submissions.  
Note that all the Dedalus protocols still use Scala clients and state machine replicas.


## Protocol implementations
The various components of each protocol.
- **Dedalus implementation**: The protocol's Dedalus code, embedded in Rust (within `datalog!` macros). The remainder of the Rust code exists to supply Dedalus with configuration parameters (such as each node's ID), connect input and output channels, and serialize/deserialize messages.
- **Scala implementation**: The same protocol implemented in Scala for comparison against the implementations in [Compartmentalized Paxos](https://mwhittaker.github.io/publications/compartmentalized_paxos.pdf). Also includes clients, state machine replicas, and protobufs, which are shared between the Dedalus and Scala implementations.
- **Scala configuration parameters**: Provides the Scala programs with configuration parameters.
- **Benchmark setup script**: Runs each component and connects ports between programs.
- **Benchmark configuration**: Where we specify the number of nodes to execute with, how long to run the benchmarks, the size of the benchmark payloads, etc.
- **Cluster configuration**: Specifies whether to run locally or in the cloud, which VM image to launch with, what network to use, and whether to use the Rust or Scala implementations.
- **Benchmark scripts**: Runs the benchmarks with the cluster config.


### Voting
Dedalus implementations, embedded in Rust: [Voting](rust/examples/voting), [AutoVoting](rust/examples/autovoting).  
Scala implementation, including the protobuf file: [Voting](shared/src/main/scala/frankenpaxos/voting).  
Scala configuration parameters: [Voting](jvm/src/main/scala/frankenpaxos/voting).  
Benchmark setup script: [voting.py](benchmarks/voting/voting.py), [autovoting.py](benchmarks/autovoting/autovoting.py).  
Benchmark configuration: [dedalus_voting_benchmark.py](benchmarks/vldb24/dedalus_voting_benchmark.py), [dedalus_autovoting_benchmark.py](benchmarks/vldb24/dedalus_autovoting_benchmark.py).  
Cluster configuration: [dedalus_voting_config.json](benchmarks/vldb24/clusters/dedalus_voting_config.json), [dedalus_autovoting_config.json](benchmarks/vldb24/clusters/dedalus_autovoting_config.json).
Benchmark scripts: [dedalus_voting_run.sh](benchmarks/vldb24/dedalus_voting_run.sh), [dedalus_autovoting_run.sh](benchmarks/vldb24/dedalus_autovoting_run.sh).



### 2PC
Dedalus implementations, embedded in Rust: [2PC](rust/examples/twopc), [Auto2PC](rust/examples/autotwopc).  
Scala implementation, including the protobuf file: [2PC](shared/src/main/scala/frankenpaxos/twopc).  
Scala configuration parameters: [2PC](jvm/src/main/scala/frankenpaxos/twopc).  
Benchmark setup script: [twopc.py](benchmarks/twopc/twopc.py), [autotwopc.py](benchmarks/autotwopc/autotwopc.py).  
Benchmark configuration: [dedalus_twopc_benchmark.py](benchmarks/vldb24/dedalus_twopc_benchmark.py), [dedalus_autotwopc_benchmark.py](benchmarks/vldb24/dedalus_autotwopc_benchmark.py).  
Cluster configuration: [dedalus_twopc_config.json](benchmarks/vldb24/clusters/dedalus_twopc_config.json), [dedalus_autotwopc_config.json](benchmarks/vldb24/clusters/dedalus_autotwopc_config.json).  
Benchmark scripts: [dedalus_twopc_run.sh](benchmarks/vldb24/dedalus_twopc_run.sh), [dedalus_autotwopc_run.sh](benchmarks/vldb24/dedalus_autotwopc_run.sh).

Our implementation of 2PC includes performs Presumed Abort logging.

### MultiPaxos
Dedalus implementations, embedded in Rust: [MultiPaxos](rust/examples/multipaxos), [AutoMultiPaxos](rust/examples/automultipaxos), [Compartmentalized Paxos](rust/examples/comppaxos).  
Scala implementation of both MultiPaxos and Compartmentalized Paxos, including the protobuf file: [MultiPaxos](shared/src/main/scala/frankenpaxos/multipaxos).  
Scala configuration parameters: [MultiPaxos](jvm/src/main/scala/frankenpaxos/multipaxos).  
Benchmark setup script: [multipaxos.py](benchmarks/multipaxos/multipaxos.py), [automultipaxos.py](benchmarks/automultipaxos/automultipaxos.py), [comppaxos.py](benchmarks/comppaxos/comppaxos.py).  
Benchmark configuration: [dedalus_multipaxos_benchmark.py](benchmarks/vldb24/dedalus_multipaxos_benchmark.py), [dedalus_automultipaxos_benchmark.py](benchmarks/vldb24/dedalus_automultipaxos_benchmark.py), [dedalus_comppaxos_benchmark.py](benchmarks/vldb24/dedalus_comppaxos_benchmark.py).  
Cluster configuration: [dedalus_multipaxos_config.json](benchmarks/vldb24/clusters/dedalus_multipaxos_config.json), [dedalus_automultipaxos_config.json](benchmarks/vldb24/clusters/dedalus_automultipaxos_config.json), [dedalus_comppaxos_config.json](benchmarks/vldb24/clusters/dedalus_comppaxos_config.json).  
Benchmark scripts: [dedalus_multipaxos_run.sh](benchmarks/vldb24/dedalus_multipaxos_run.sh), [dedalus_automultipaxos_run.sh](benchmarks/vldb24/dedalus_automultipaxos_run.sh), [dedalus_comppaxos_run.sh](benchmarks/vldb24/dedalus_comppaxos_run.sh).

### Microbenchmarks
Dedalus implementations, embedded in Rust: [Mutually independent decoupling (before)](rust/examples/decoupling_mutually_independent/), [Mutually independent decoupling (after)](rust/examples/auto_decoupling_mutually_independent/), [Functional decoupling (before)](rust/examples/decoupling_functional/), [Functional decoupling (after)](rust/examples/auto_decoupling_functional/), [Monotonic decoupling (before)](rust/examples/decoupling_monotonic/), [Monotonic decoupling (after)](rust/examples/auto_decoupling_monotonic/), [State machine decoupling (before)](rust/examples/decoupling_state_machine/), [State machine decoupling (after)](rust/examples/auto_decoupling_state_machine/), [General decoupling (before)](rust/examples/decoupling_general/), [General decoupling (after)](rust/examples/auto_decoupling_general/), [Partitioning with co-hashing (before)](rust/examples/partitioning_cohashing/), [Partitioning with co-hashing (after)](rust/examples/auto_partitioning_cohashing/), [Partitioning with dependencies (before)](rust/examples/partitioning_dependencies/), [Partitioning with dependencies (after)](rust/examples/auto_partitioning_dependencies/), [Partial partitioning (before)](rust/examples/partitioning_partial/), [Partial partitioning (after)](rust/examples/auto_partitioning_partial/).  
Scala clients: [automicrobenchmarks](shared/src/main/scala/frankenpaxos/automicrobenchmarks).  
Scala configuration parameters: [automicrobenchmarks](jvm/src/main/scala/frankenpaxos/automicrobenchmarks).  
Benchmark setup script: [automicrobenchmarks.py](benchmarks/automicrobenchmarks/automicrobenchmarks.py).  
Benchmark configuration: [dedalus_microbenchmarks_benchmark.py](benchmarks/vldb24/dedalus_microbenchmarks_benchmark.py).  
Cluster configuration: [dedalus_microbenchmarks_config.json](benchmarks/vldb24/clusters/dedalus_microbenchmarks_config.json).  
Benchmark scripts: [dedalus_microbenchmarks_run.sh](benchmarks/vldb24/dedalus_microbenchmarks_run.sh).

### PBFT (critical path)
Dedalus implementations, embedded in Rust: [PBFT](rust/examples/pbft_critical_path), [AutoPBFT](rust/examples/autopbft_critical_path).  
Scala clients: [MultiPaxos](shared/src/main/scala/frankenpaxos/multipaxos). This is not a typo; PBFT uses the same clients as Paxos with different flags.  
Benchmark setup script: [pbft_critical_path.py](benchmarks/pbft/pbft_critical_path.py), [autopbft_critical_path.py](benchmarks/autopbft/autopbft_critical_path.py).  
Benchmark configuration: [dedalus_pbft_critical_path_benchmark.py](benchmarks/vldb24/dedalus_pbft_critical_path_benchmark.py), [dedalus_autopbft_critical_path_benchmark.py](benchmarks/vldb24/dedalus_autopbft_critical_path_benchmark.py).  
Cluster configuration: [dedalus_pbft_config.json](benchmarks/vldb24/clusters/dedalus_pbft_config.json), [dedalus_autopbft_config.json](benchmarks/vldb24/clusters/dedalus_autopbft_config.json).  
Benchmark scripts: [dedalus_pbft_critical_path_run.sh](benchmarks/vldb24/dedalus_pbft_critical_path_run.sh), [dedalus_autopbft_critical_path_run.sh](benchmarks/vldb24/dedalus_autopbft_critical_path_run.sh).

This implementation of PBFT does not contain view-change or checkpointing.
There is no corresponding Scala implementation (we only created Scala implementations for the SIGMOD '24 paper, and this was implemented just for PaPoC '24).

## Writing your own protocols and benchmarks