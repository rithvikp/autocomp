#!/bin/bash

set -exuo pipefail

rustup run nightly python -m benchmarks.vldb24.dedalus_automultipaxos_benchmark -j /mnt/nfs/tmp/frankenpaxos-assembly-0.1.0-SNAPSHOT.jar -m -s /mnt/nfs/tmp/ -l info --cluster_config ../clusters/automultipaxos/dedalus_local_hydro_config.json
