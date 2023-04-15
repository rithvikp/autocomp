#!/bin/bash

set -exuo pipefail

rustup run nightly python -m benchmarks.vldb24.multipaxos_lt_dedalus.benchmark -j /mnt/nfs/tmp/frankenpaxos-assembly-0.1.0-SNAPSHOT.jar -m -s /tmp/ -l info --cluster_config ../clusters/multipaxos/dedalus_local_hydro_config.json