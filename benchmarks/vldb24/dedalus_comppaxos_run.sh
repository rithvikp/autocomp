#!/bin/bash

set -exuo pipefail

RUST_BACKTRACE=1 rustup run nightly python -m benchmarks.vldb24.dedalus_comppaxos_benchmark -j /mnt/nfs/tmp/frankenpaxos-assembly-0.1.0-SNAPSHOT.jar -m -s /mnt/nfs/tmp/ -l info --cluster_config ../clusters/comppaxos/dedalus_hydro_config.json