# VLDB '24 Submission

This directory contains the benchmark scripts used in our VLDB'24 submission. For our protocol implementations, refer to the top-level README.

Each `*_benchmark.py` script defines a single benchmark (which may include hyperparameter searches and multiple trials). The plotting code and results are in [`plots/`](plots/).

The benchmarks automatically provision cloud machines as necessary, using the [Hydro CLI](https://github.com/hydro-project/hydroflow/tree/main/hydro_cli). When running these benchmarks, use this [branch](https://github.com/hydro-project/hydroflow/tree/rithvik/autocomp_adjustments) of Hydroflow.


To run a specific benchmark, run the corresponding `*_run.sh` file. Note that the path to the jar file in these scripts may need to be adjusted if you are running locally without the NFS. Each script also expects a JSON cluster configuration with information about the cloud provider etc. Here is an example for Dedalus MultiPaxos (`<worker image>` is the name of a GCP disk image that has access to the NFS as well as Java 8):
```
{
    "mode": "hydro",
    "env": {
        "cloud": "gcp",
        "project":  <GCP project name>,
        "machine_type": "n2-standard-4",
        "scala_image": <worker image>,
        "hydroflow_image": <worker image>,
        "zone": "us-west1-b",
        "vpc": "eval",
        "user": <worker image username>
    },
    "services": {
        "clients": {
            "type": "scala"
        },
        "leaders": {
            "type": "hydroflow",
            "rust_example": "multipaxos"
        },
        "acceptors": {
            "type": "hydroflow",
            "rust_example": "multipaxos"
        },
        "replicas": {
            "type": "scala"
        }
    }
}
```