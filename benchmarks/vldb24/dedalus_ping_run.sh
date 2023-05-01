#!/bin/bash

set -exuo pipefail

rustup run nightly python -m benchmarks.vldb24.dedalus_ping_benchmark -j /mnt/nfs/tmp/frankenpaxos-assembly-0.1.0-SNAPSHOT.jar -s /mnt/nfs/tmp/ -l info --cluster_config ../clusters/echo/dedalus_hydro_config.json