#!/bin/bash

set -exuo pipefail

RUST_BACKTRACE=1 rustup run nightly-2023-04-18 python -m benchmarks.vldb24.dedalus_comppaxos_benchmark -j /mnt/nfs/tmp/frankenpaxos-assembly-0.1.0-SNAPSHOT.jar -m -s /mnt/nfs/tmp/ -l info --cluster_config benchmarks/vldb24/clusters/dedalus_comppaxos_config.json