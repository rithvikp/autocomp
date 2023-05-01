# Autocomp

This repository contains implementations of various state machine replication protocols in both Scala and Dedalus. It was initially forked from [frankenpaxos](https://github.com/mwhittaker/frankenpaxos) by Michael Whittaker.

Core scala code is in the [`shared`](shared/) directory, core rust code is in the [`rust`](rust/) directory, JVM-specific code is in the
[`jvm/`](jvm/) directory, and Javascript-specific code is in the [`js/`](js/) directory.

For the benchmark scripts used in our VLDB '24 submission, please look at [`benchmarks/vldb24/README.md`](benchmarks/vldb24/README.md). To view the implementations of the protocols in this submission, refer to the next section.

## Protocols
Note that all the Dedalus protocols still use Scala clients.

### Voting
For the Scala implementation of voting, refer to [shared/src/main/scala/frankenpaxos/voting](shared/src/main/scala/frankenpaxos/voting).
For the Dedalus implementation, refer to [rust/examples/voting](rust/examples/voting). For the Dedalus implementation of AutoVoting, refer to [rust/examples/autovoting](rust/examples/autovoting).

### 2PC
For the Scala implementation of 2PC, refer to [shared/src/main/scala/frankenpaxos/twopc](shared/src/main/scala/frankenpaxos/twopc). For the Dedalus implementation, refer to [rust/examples/twopc](rust/examples/twopc). For the Dedalus implementation of Auto2PC, refer to [rust/examples/autotwopc](rust/examples/autotwopc).

### Multipaxos
For the Scala implementation of Multipaxos, refer to [shared/src/main/scala/frankenpaxos/multipaxos](shared/src/main/scala/frankenpaxos/multipaxos). Note that this implementation can run both regular MultiPaxos and [Compartmentalized
MultiPaxos](https://mwhittaker.github.io/publications/compartmentalized_paxos.pdf).

For the Dedalus implementation of MultiPaxos, refer to [rust/examples/multipaxos](rust/examples/multipaxos). For the Dedalus implementation of AutoMultiPaxos, refer to [rust/examples/automultipaxos](rust/examples/automultipaxos). Finally, for the Dedalus implementation of Compartmentalized MultiPaxos, refer to [rust/examples/comppaxos](rust/examples/comppaxos).

### Microbenchmarks
The client for microbenchmarks are implemented in [shared/src/main/scala/frankenpaxos/automicrobenchmarks](shared/src/main/scala/frankenpaxos/automicrobenchmarks). The microbenchmark protocols are implemented in [rust/examples/](rust/examples/), with one directory for each microbenchmark and another for its optimized variant (ex. `decoupling_functional` and `auto_decoupling_functional`).


## Getting Started with Scala Code
Make sure you have Java, Scala, and sbt installed. We tested everything with
Java version 1.8.0_131 and with sbt version 1.1.6 on Ubuntu 18.04. You can
install these however you like (e.g., [this
script](https://raw.githubusercontent.com/mwhittaker/vms/master/install_java8.sh)
and [this
script](https://raw.githubusercontent.com/mwhittaker/vms/master/install_scala.sh)).

You can build and run all of the Scala code using `sbt`.