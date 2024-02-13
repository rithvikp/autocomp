# Protocols
All protocols used in the SIGMOD '24 and PaPoC '24 submissions.  
Note that all the Dedalus protocols still use Scala clients and state machine replicas.

Jump to sections below:
- [Running the benchmarks](#running-the-benchmarks)
- [Protocol implementations](#protocol-implementations)
- [Writing your own protocols and benchmarks](#writing-your-own-protocols-and-benchmarks)


## Running the benchmarks
We will assume you have VSCode installed locally.

### Setting up VSCode
On your local machine, run the following command so the SSH keys can be added to your computer and you can use VSCode to connect into the VMs:
```bash
gcloud compute config-ssh
```
> [!NOTE]  
> You will need to run the command above EACH TIME THE VM RESTARTS if you want to connect to the VM, because the public IP of the VM will change.

Open VSCode on your computer, click the blue `><` box in the bottom left corner, select "Connect to Host", select `eval-primary...`, then enter your SSH password (as many times as prompted). You should now be connected to the VM.

Click "Open folder" on the left panel, and open the `autocomp` directory. Install the `rust analyzer` extension on the remote machine for compile-time typing and error checking.

### Running your first experiment
Modify the cluster scripts.  
`<cloud>`: `local` if you want to run it on `eval-primary`, `gcp` on VMs.  
`<project>`: Your GCP project name.  
`<zone>`: The zone where your VMs and networks are located.  
`<username>`: Your username on the VMs.

```bash
cd ~/autocomp/benchmarks/vldb24/clusters
python modify_clusters.py --cloud <cloud> --project <project> --zone <zone> --user <username>
```

Decide the specific protocol [below](#protocol-implementations) you wish to run, and modify its benchmark configuration as you see fit.
In general, here's how each benchmark's configuration can be modified:
1. You will find multiple `for` loop iterating over arrays at the end of each file's `def inputs()` function. Each tuple in each array represents a different run configuration. The total number of runs is the number of combinations of tuples across the arrays.
2. If you modify the number of nodes in these arrays, then you **must** also modify the `def cluster_spec()` function at the top of the file so it equals the **maximum** number of nodes used for that type in the entire benchmark. For example, if you have `for num_replicas in [1,3,5]` in the `index()` function, then you must have `'replicas': 5` in the `cluster_spec()` function.
3. You may find a `*3` at the end of the returned array. This represents running each experiment 3 times. This can be modified as well.

Now run the benchmark script from the `~/autocomp` directory. For example, to run the MultiPaxos Dedalus implementation, run:
```bash
cd ~/autocomp
benchmarks/vldb24/dedalus_multipaxos_run.sh
```

### Stopping the experiments
The experiments should stop on their own. If you need to stop them early, type `Ctrl+C`. Hydro CLI should also kill the related VMs on shutdown, but you should always check if there are any VMs still running in [the console](https://console.cloud.google.com/compute/instances). If Hydro CLI shut down incorrectly and you need to kill VMs, you can try killing them with Terraform:
```bash
cd ~/autocomp/.hydro
cd .tmp*
terraform destroy -auto-approve
```
Where the folder's name begins with `.tmp`.  
If that doesn't work, then you can always manually kill the instances from the console.

### Interpreting benchmark results

You can find benchmark results in /mnt/nfs/tmp, where each experimental result is stored in a different directory.
Within the directory, `results.csv` contains the complete results for all experiments; we copied representative results from different experiments to [benchmarks/vldb24/plots](benchmarks/vldb24/plots) and turned them into graphs by running the Python scripts in those paths.

The results for each configuration's run can be found inside each numbered folder, starting with `000`. From there, you can inspect the outputs of each process and the ports they used.
If you wish to debug the results, refer to [FAQ](FAQ.md).


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
Benchmark scripts: [dedalus_voting_run.sh](benchmarks/vldb24/dedalus_voting_run.sh), [dedalus_autovoting_run.sh](benchmarks/vldb24/dedalus_autovoting_run.sh), [scala_voting_run.sh](benchmarks/vldb24/scala_voting_run.sh).



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

### 1. Decide what your clients will do
Your life will be easier if you reuse clients that are already used in other protocols. For example, PBFT uses Paxos clients with slight modifications. If you wish to create new clients, you can start by copying a simple implementation from [automicrobenchmarks](shared/src/main/scala/frankenpaxos/automicrobenchmarks) and creating a new directory under [shared/src/main/scala/frankenpaxos](shared/src/main/scala/frankenpaxos).

Be sure to recompile Scala every time you modify it:
```bash
cd ~/autocomp
./scripts/assembly_without_tests.sh
cp jvm/target/scala-2.12/frankenpaxos-assembly-0.1.0-SNAPSHOT.jar /mnt/nfs/tmp
```

### 2. The Rust wrapper
Create a new directory under rust/examples (by copying over another one), and modify `main.rs` as necessary.
Create a new Rust file for each component of your protocol.
Each component, if it accepts configuration parameters, should have a struct similar to this at the top of the file:
```rust
#[derive(clap::Args, Debug)]
pub struct LeaderArgs {
    #[clap(long = "leader.f")]
    f: u32
}
```
Add your own arguments as necessary.
Keep track of these arguments; you will need to pass them in later from the benchmark configuration.

If this component sends or receives protobuf messages from Scala, you will need to serialize and deserialize them. Refer to how it's handled in [PBFT](https://github.com/rithvikp/autocomp/blob/27127efec52ba39f1c31cd5d4b9076c26eb6aedd/rust/examples/pbft_critical_path/pbft_replica.rs#L45) for help.

The core functionality of each component is contained under the ```run``` function:
```rust
pub async fn run(cfg: PBFTReplicaArgs, mut ports: HashMap<String, ServerOrBound>)
```

You will need to set up a send or receive port for every Dedalus input/output channel.
Receive ports follow this structure:
```rust
let client_recv = ports
    .remove("receive_from$clients$0")
    .unwrap()
    .connect::<ConnectedTagged<ConnectedBidi>>()
    .await
    .into_source();
```
Send ports follow this structure:
```rust
let replica_send = ports
    .remove("send_to$replicas$0")
    .unwrap()
    .connect::<ConnectedDemux<ConnectedBidi>>()
    .await
    .into_sink();
```
If your Dedalus code needs access to the IDs of the nodes it's sending to, refer to how it's done [here](https://github.com/rithvikp/autocomp/blob/27127efec52ba39f1c31cd5d4b9076c26eb6aedd/rust/examples/multipaxos/leader.rs#L95).

The `String` inside `remove()` identifies the name of the component that this component sends to or receives from, in addition to an `int`, starting from 0, if the same component sends/receives from the other component on multiple channels. For example, Paxos leaders receive messages from acceptors on 3 separate channels, so the `String` would be [`receive_from$acceptors$0`](https://github.com/rithvikp/autocomp/blob/27127efec52ba39f1c31cd5d4b9076c26eb6aedd/rust/examples/multipaxos/leader.rs#L9), [`receive_from$acceptors$1`](https://github.com/rithvikp/autocomp/blob/27127efec52ba39f1c31cd5d4b9076c26eb6aedd/rust/examples/multipaxos/leader.rs#L106), and [`receive_from$acceptors$2`](https://github.com/rithvikp/autocomp/blob/27127efec52ba39f1c31cd5d4b9076c26eb6aedd/rust/examples/multipaxos/leader.rs#L120).  
Keep track of the names used here for the other components; you will connect them by name in the benchmark configuration.

Create a variable for each configuration parameter your Dedalus code should have access to, as seen [here](https://github.com/rithvikp/autocomp/blob/27127efec52ba39f1c31cd5d4b9076c26eb6aedd/rust/examples/pbft_critical_path/pbft_replica.rs#L289).
For example, if your code waits for a quorum of `2f+1` messages, it needs to know `f`.


### 3. Implementing the protocol in Dedalus
You are now ready to implement your protocol in Dedalus. 
If you do not know Dedalus, please read [Dedalus: Datalog in Time and Space](https://www2.eecs.berkeley.edu/Pubs/TechRpts/2009/EECS-2009-173.pdf).
We will be using sugared syntax that omits time and location from tuples.
We will go through the Dedalus syntax accepted by Hydroflow example by example.

#### EDBs (constants)
Constants in Dedalus are just relations that output the same fact at every tick. They are defined as `.input <dedalus_name> <rust_source>`:
```prolog
.input id `repeat_iter([(my_id,),])`
.input pbftReplicas `repeat_iter(pbft_replicas.clone()) -> map(|p| (p,))`
```
If the constant is already a single value (such as `my_id` above, which is a `u32` from [here](https://github.com/rithvikp/autocomp/blob/27127efec52ba39f1c31cd5d4b9076c26eb6aedd/rust/examples/pbft_critical_path/pbft_replica.rs#L289)), you will need to wrap it in a tuple of a single element, then in an array of a single element, as seen in the 1st line.  
If the constant is an array, you need to clone it and then map each element to a tuple, as seen in the 2nd line.


#### Timers
Timers can be implemented with `.input`. See how it's used in the [Paxos leader](https://github.com/rithvikp/autocomp/blob/27127efec52ba39f1c31cd5d4b9076c26eb6aedd/rust/examples/multipaxos/leader.rs#L215).


#### Inputs and outputs
Message channels are defined as `.async <dedalus_name> <rust_sink> <rust_source>`.  
If it's an input channel, then `<rust_sink>` is empty, and `<rust_source>` will be structured as `source_stream(<input_channel>) -> filter_map(|x| ...)`.   
If it is an output channel, then `<rust_source>` is empty, and `<rust_sink>` will be structured as `map(|(dest_id, x)| ...) -> dest_sink(<output_channel>)`.  
The following lines are copied from [Voting](https://github.com/rithvikp/autocomp/blob/27127efec52ba39f1c31cd5d4b9076c26eb6aedd/rust/examples/voting/leader.rs):
```prolog
.async clientIn `` `source_stream(client_recv) -> map(|x| deserialize(x.unwrap(), &vote_requests))`
.async clientOut `map(|(node_id, (v,)):(u32,(Rc<Vec<u8>>,))| (node_id, serialize(v))) -> dest_sink(client_send)` ``
.async voteToReplica `map(|(node_id, v):(u32,(u32,i64,Rc<Vec<u8>>,))| (node_id, serialize_to_bytes(v))) -> dest_sink(to_replica_sink)` ``
.async voteFromReplica `` `source_stream(from_replica_source) -> map(|v| deserialize_from_bytes::<(u32,u32,i64,Rc<Vec<u8>>,)>(v.unwrap().1).unwrap())`
```
The first 2 lines are communications with a Scala client; the second 2 are communications between Dedalus nodes.

A message received from an output channel will be of the type `(sender_id, msg)`, and an output that is sent on an input channel must be of the type `(receiver_id, msg)`.
The type of the message is `BytesMut` if it is a protobuf message to/from the client; otherwise, there are no restrictions.
There are built-in `serialize_to_bytes()` and `deserialize_from_bytes()` functions that can be used to communicate between Dedalus nodes; the exact types of the message need to be specified between the `<>`.

To send a message on an output channel, we must use the following syntax:
```prolog
voteToReplica@addr(client, id, v) :~ clientIn(client, id, v), replicas(addr)
```
This sends the message `(client, id, v)` to `addr` on the `voteToReplica` channel, which connects to `to_replica_sink` above.


#### Dedalus syntax
Regular rules have similar syntax to most Datalog textbooks:
```prolog
r(a) :- s(a), t(a)
```
Unused fields can be omitted with `_`.

Negation uses `!`:
```prolog
r(a) :- s(a), !t(a)
```
Boolean relations do not need to have attributes. For example:
```prolog
IsLeader() :- leaderId(i), id(i)
```

Persisted relations `r` require an extra line:
```prolog
.persist r
```

Synchronous rules (where facts arrive at the head at time `t+1`) replace `:-` with `:+`.

Basic arithmetic can be performed with `+`, `-`, `*`, and `%` (mod) in the head of rules, with support for parentheses.
Comparisons can be performed with `==`, `!=`, `<`, `>`, `<=`, and `>=` in the body of rules.
We also support the following aggregations: `count()`, `min()`, `max()`, `sum()`, `choose()`, and `index()`.
`index()` is a special aggregation that assigns each tuple in the relation, for that tick, a unique index, without accounting for any group-bys. See its usage [here](https://github.com/rithvikp/autocomp/blob/27127efec52ba39f1c31cd5d4b9076c26eb6aedd/rust/examples/autopbft_critical_path/leader.rs#L154).
Find out more about the grammar [here](https://github.com/hydro-project/hydroflow/blob/main/hydroflow_datalog_core/src/grammar.rs).

Sometime type inference can fail, and you will need to specify the type of a relation manually. [Here](https://github.com/rithvikp/autocomp/blob/27127efec52ba39f1c31cd5d4b9076c26eb6aedd/rust/examples/autopbft_critical_path/leader.rs#L140) is an example.

As always, be sure to avoid negations and aggregations in the same stratum.


### 4. Creating the benchmark setup script
Now that your implementation is complete, you will need to first connect the components together with a benchmark setup script. Create a new folder under [benchmarks](benchmarks) for your protocol, then copy over the main file from another protocol that uses the same client. We will use [pbft_critical_path.py](https://github.com/rithvikp/autocomp/blob/master/benchmarks/pbft/pbft_critical_path.py) as an example.

Specify configuration parameters at the top of the file under:
```python
class Input(NamedTuple):
```

Throughout this file, rename any references to the PBFT classes, such as `DedalusPBFTCriticalPathOutput` or `DedalusPBFTCriticalPathNet`, to your protocol.

##### Network
Add your components below:
```python
class Placement(NamedTuple):
```
And assign it ports in the return functions for `prom_placement()` and `placement()`:
```python
return self.Placement(
    clients=portify('clients', self._input.num_client_procs),
    pbft_replicas=portify('pbft_replicas', self._input.num_pbft_replicas),
    replicas=portify('replicas', self._input.num_replicas),
)
```
Also add it to the object returned in `config()`:
```python
'leader_address': [{
    'host': e.host.ip(),
    'port': e.port
} for e in self.placement(index=index).pbft_replicas]
```

> [!NOTE]  
> If you create a component here that did not exist before, you will need to add it to your protocol's Scala client's `Config.proto` file under [jvm/src/main/scala/frankenpaxos](jvm/src/main/scala/frankenpaxos). Then, you must recompile Scala and move the `.jar` to mnt/nfs/tmp, as described [here](#1-decide-what-your-clients-will-do).

##### Suite
For every component, launch it by writing code similar to this, replacing all references to `pbft_replicas` with references to your component:
```python
if self.service_type("pbft_replicas") == "hydroflow":
    pbft_replica_procs: List[proc.Proc] = []
    for (i, pbft_replica) in enumerate(net.prom_placement().pbft_replicas):
        pbft_replica_procs.append(self.provisioner.popen_hydroflow(bench, f'pbft_replicas_{i}', input.f, [
            '--service',
            'pbft',
            '--pbft_replica.index',
            str(i),
            '--pbft_replica.f',
            str(input.f),
            '--prometheus-host',
            pbft_replica.host.ip(),
            '--prometheus-port',
            str(pbft_replica.port),
        ]))
else:
    raise ValueError("DedalusPBFTCriticalPathSuite only supports hydroflow pbft_replicas")
```
This is also where you supply configuration parameters. Note that the `service` name **must** match the lower case name defined in `enum Service` in `main.rs`: for example, `--service pbft` matches [this](https://github.com/rithvikp/autocomp/blob/27127efec52ba39f1c31cd5d4b9076c26eb6aedd/rust/examples/pbft_critical_path/main.rs#L6). All parameter names must match their definitions in the component's rust file: for example, `--pbft_replica.index` matches [this](https://github.com/rithvikp/autocomp/blob/27127efec52ba39f1c31cd5d4b9076c26eb6aedd/rust/examples/pbft_critical_path/pbft_replica.rs#L18).  
If you created new Scala components, you can copy similar code for launching Scala clients or replicas.


Connect your components by finding `endpoints, receive_endpoints = self.provisioner.rebuild` and modifying the following lines to suit your components:
```python
endpoints, receive_endpoints = self.provisioner.rebuild(1, {
    "clients": ["pbft_replicas"],
    "pbft_replicas": ["pbft_replicas", "pbft_replicas", "pbft_replicas", "replicas"],
    "replicas": ["clients"],
},
{
    "clients": input.num_client_procs,
    "replicas": input.num_replicas,
})
```
The pattern is `"sender": ["receiver1", "receiver2", etc]`. List the same receiver multiple times (as seen above) if there are multiple channels from that sender to that receiver. Don't modify the 2nd object (`receive_endpoints`).

Add your components to the Prometheus config after the `# Launch Prometheus` comment, replacing the name and array with your component's:
```python
'multipaxos_client': [
    f'{e.host.ip()}:{e.port}'
    for e in net.prom_placement().clients
]
```

Replace the `--receive_addrs` parameter for clients with the component that the clients send to:
```python
'--receive_addrs',
','.join([x.host.ip()+":"+str(x.port) for x in net.placement(index=i).pbft_replicas])
```

Your benchmark setup script should be ready to go!


### 5. Creating the cluster configuration

Create a new file under [benchmarks/vldb24/clusters](benchmarks/vldb24/clusters) for your protocol, by copying over the config from another protocol that uses your client. Add any new components under `services`, give it the same name as the rust file, with an extra "s" at the end, and pointing it to the correct folder under `rust_example`. For example, for [rust/examples/pbft_critical_path/pbft_replica.rs](rust/examples/pbft_critical_path/pbft_replica.rs):
```json
"pbft_replicas": {
    "type": "hydroflow",
    "rust_example": "pbft_critical_path"
}
```


### 6. Creating the benchmark configuration

Create a new `_benchmark.py` file under [benchmarks/vldb24](benchmarks/vldb24) for your protocol by copying over another protocol's. Modify the import statement at the top to point to your benchmark setup file and rename everything in the file to suit your protocol.

Modify `cluster_spec()` so there's a field for every component. Modify the `Input` returned by `inputs()` so it contains any new configuration parameters you defined. Also add those parameters to the `summary()` function at the bottom so they will be printed out.


### 7. Creating the benchmark script
Create a new `_run.sh` file under [benchmarks/vldb24](benchmarks/vldb24/) for your protocol by copying over another protocol's. Modify it so it uses your benchmark configuration Python file and your cluster config.

You're ready to run the benchmark! See [FAQ](FAQ.md) if you run into problems.