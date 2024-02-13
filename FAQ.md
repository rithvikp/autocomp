# FAQ
Your experiment didn't work and you'd like to know why. You should contact David or Rithvik, since this codebase has lots of things working together and it's tricky to debug. If you're on your own, here's some useful tips, assuming you followed the [setup](SETUP.md) and [protocol](PROTOCOLs.md) instructions:

### My results are bad: Profiling with Prometheus
If your results are much worse than expected, you can inspect the metrics across time collected by Prometheus. Head to your benchmark results in /mnt/nfs/tmp, go to the specific run's folder (such as `001`), then run:
```bash
prometheus --config.file=<(echo "") --storage.tsdb.path=prometheus_data --web.listen-address=0.0.0.0:9090
```

Then, on your local computer, run the following, where `<ip>` is the external IP of `eval-primary`, assuming you can SSH into `eval-primary`:
```bash
ssh -L 127.0.0.1:9090:localhost:9090 <ip>
```

Then navigate to `localhost:9090` in your browser, click the Graph tab, and check metrics. Metrics can be logged in Dedalus like [so](https://github.com/rithvikp/autocomp/blob/27127efec52ba39f1c31cd5d4b9076c26eb6aedd/rust/examples/voting/leader.rs#L47), and in Scala like [so](https://github.com/rithvikp/autocomp/blob/27127efec52ba39f1c31cd5d4b9076c26eb6aedd/shared/src/main/scala/frankenpaxos/multipaxos/Client.scala#L378). Feel free to add your own metric measurements; they are relatively lightweight.


### Workers are crashing immediately
Create a new VM instance using `worker-image`, and check if it has access to /mnt/nfs/tmp, and can run `java` and `prometheus` from the command line. Then, check if `eval-primary` can SSH into itself and also run `java` and `prometheus` with the following, replacing `<username>` with the username on `eval-primary`:
```bash
ssh -i ~/.ssh/id_ed25519 <username>@localhost java
ssh -i ~/.ssh/id_ed25519 <username>@localhost prometheus
```


### "port already in use"
This is usually caused by:
1. Prometheus attempted to start on a port that conflicts with other processes, because the configuration you're trying to launch is too large, as seen [here](https://github.com/rithvikp/autocomp/blob/27127efec52ba39f1c31cd5d4b9076c26eb6aedd/benchmarks/autopbft/autopbft_critical_path.py#L137). Start Prometheus on a higher port number (like the 60000s).
2. Zombie processes from previous runs. Find them with `ps aux` and kill them one by one, or if you can't, restart the VM.


### My quorum-based protocol is performing poorly
Try waiting for all messages to arrive (2f+1 for Paxos, for example), instead of a quorum. This should simplify garbage collection logic, and make Dedalus way more performant for some reason.

Alternatively, check if you're using a relation that is persisted with `.persist` in the body of any rule. Since those relations grow monotonically, the evaluation of those rules will eventually slow down over time and should be avoided.


### My Dedalus has weird typing errors
Check that all your types are correct (did you put 2 different types in the same attribute of a relation, or use the same relation with a different number of attributes). If so, type inference may have failed, so you should try manual annotation, like in [this example](https://github.com/rithvikp/autocomp/blob/27127efec52ba39f1c31cd5d4b9076c26eb6aedd/rust/examples/autopbft_critical_path/leader.rs#L140) where the type of `nextSlot` is manually annotated.