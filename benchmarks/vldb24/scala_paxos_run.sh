#!/bin/bash

set -exuo pipefail

rustup run nightly-2023-04-18 python -m benchmarks.vldb24.scala_paxos_benchmark -j /mnt/nfs/tmp/frankenpaxos-assembly-0.1.0-SNAPSHOT.jar -m -s /mnt/nfs/tmp/ -l info --cluster_config benchmarks/vldb24/clusters/scala_multipaxos_config.json