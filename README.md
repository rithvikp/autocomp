# Autocomp

This repository contains implementations of various state machine replication protocols in both Scala and Dedalus. It was initially forked from [frankenpaxos](https://github.com/mwhittaker/frankenpaxos) by Michael Whittaker.

Core scala code is in the [`shared`](shared/) directory, core rust code is in the [`rust`](rust/) directory, JVM-specific code is in the
[`jvm/`](jvm/) directory, and Javascript-specific code is in the [`js/`](js/) directory.

For the benchmark scripts used in our VLDB '24 submission, as well as the technical report, please look at [`benchmarks/vldb24/README.md`](benchmarks/vldb24/README.md). To view the implementations of the protocols in this submission, refer to the next section.

## Protocols
Note that all the Dedalus protocols still use Scala clients.

### Voting
For the Dedalus implementation, refer to [rust/examples/voting](rust/examples/voting). For the Dedalus implementation of AutoVoting, refer to [rust/examples/autovoting](rust/examples/autovoting).
For the Scala implementation of voting, refer to [shared/src/main/scala/frankenpaxos/voting](shared/src/main/scala/frankenpaxos/voting).

### 2PC
For the Dedalus implementation, refer to [rust/examples/twopc](rust/examples/twopc). For the Dedalus implementation of Auto2PC, refer to [rust/examples/autotwopc](rust/examples/autotwopc).
For the Scala implementation of 2PC, refer to [shared/src/main/scala/frankenpaxos/twopc](shared/src/main/scala/frankenpaxos/twopc).

### Multipaxos
For the Dedalus implementation of MultiPaxos, refer to [rust/examples/multipaxos](rust/examples/multipaxos). For the Dedalus implementation of AutoMultiPaxos, refer to [rust/examples/automultipaxos](rust/examples/automultipaxos). Finally, for the Dedalus implementation of [Compartmentalized MultiPaxos](https://mwhittaker.github.io/publications/compartmentalized_paxos.pdf), refer to [rust/examples/comppaxos](rust/examples/comppaxos).

For the Scala implementation of Multipaxos, refer to [shared/src/main/scala/frankenpaxos/multipaxos](shared/src/main/scala/frankenpaxos/multipaxos). Note that this implementation can run both regular MultiPaxos and Compartmentalized MultiPaxos.

### Microbenchmarks
The client for microbenchmarks are implemented in [shared/src/main/scala/frankenpaxos/automicrobenchmarks](shared/src/main/scala/frankenpaxos/automicrobenchmarks). The microbenchmark protocols are implemented in [rust/examples/](rust/examples/), with one directory for each microbenchmark and another for its optimized variant (ex. `decoupling_functional` and `auto_decoupling_functional`).


## Getting Started with Running Benchmarks

We run all our benchmarks on GCP through a bastion host. All machines use Ubuntu 18.04, bash, and Java 8 (the same as in the Compartmentalized MultiPaxos paper).

To setup the bastion host and reproduce our experiments, perform the following configuration. It is documented on a best-effort basis.

1. `sudo apt-get update`
1. `sudo apt-get upgrade`
1. `curl https://raw.githubusercontent.com/mwhittaker/vms/master/install_java8.sh -sSf | bash`
1. Install Coursier and Scala (and add to `~/.bash_path`)
   1. `wget https://github.com/coursier/launchers/raw/master/cs-x86_64-pc-linux-static.gz`
   1. `gzip -d`
   1. `chmod +x cs`
   1. Run `cs setup` when done
1. Source `~/.bash_path` at the top of `~/.bashrc`. This adds Java to the path even in non-interactive mode.
1. Install conda, setup a virtual environment, and add dependencies.
   1. `wget https://repo.anaconda.com/miniconda/Miniconda3-py37_23.1.0-1-Linux-x86_64.sh`
   1. `bash Miniconda3-latest-Linux-x86_64.sh`
   1. `conda create -n autocomp python=3.7`
   1. Add `conda activate autocomp` to bottom of `~/.bashrc`
1. Increase `MaxSessions` and `MaxStartups` in `/etc/ssh/sshd_config` to `100` and `100:30:200`, respectively.
1. Re-source `~/.profile` and `~/.bashrc`.
1. Install the protobuf compiler, Rust, and Prometheus.
1. Clone this repo into the VM
1. Compile the Scala code by running `./scripts/assembly_without_tests.sh`

These benchmarks also assume the existence of an NFS that all machines in the system can access. We manually run an NFS on a single VM to do so. We mount the NFS to `/mnt/nfs` on all machines and our benchmarks operate inside `/mnt/nfs/tmp`. Copy the jar from the previous compilation step into this directory.

You will also need a disk image (saved to your GCP account) that is configured with access to this NFS and also has Java 8 installed.

You will also need to clone the [Hydroflow](https://github.com/hydro-project/hydroflow) repository into the same parent directory as this repository. as well as load a wheel of Hydro CLI (accessible via GitHub Actions or by manually building it) into your Python environment.

You are now ready to run benchmarks! Refer to [benchmarks/vldb24/README.md](benchmarks/vldb24/README.md) for more information about our submission benchmarks.