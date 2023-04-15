from .. import benchmark
from .. import cluster
from .. import host
from .. import parser_util
from .. import pd_util
from .. import perf_util
from .. import proc
from .. import prometheus
from .. import proto_util
from .. import util
from .. import workload
from ..workload import Workload
from typing import Any, Callable, Collection, Dict, List, NamedTuple, Optional
import argparse
import csv
import datetime
import enum
import enum
import itertools
import os
import pandas as pd
import paramiko
import subprocess
import time
import tqdm
import yaml

# hpython -m benchmarks.voting.smoke -j /mnt/nfs/tmp/frankenpaxos-assembly-0.1.0-SNAPSHOT.jar -m -s /mnt/nfs/tmp/ -l info --cluster_config ../clusters/voting/scala_hydro_config.json

# Input/Output #################################################################
class Input(NamedTuple):
    # System-wide parameters. ##################################################
    num_clients_per_proc: int
    num_replicas: int
    jvm_heap_size: str
    log_level: str

    # Benchmark parameters. ####################################################
    duration: datetime.timedelta
    timeout: datetime.timedelta
    warmup_duration: datetime.timedelta
    warmup_timeout: datetime.timedelta
    warmup_sleep: datetime.timedelta
    client_lag: datetime.timedelta
    leader_flush_every_n: int
    profiled: bool
    monitored: bool
    prometheus_scrape_interval: datetime.timedelta


Output = benchmark.RecorderOutput


# Networks #####################################################################
class VotingNet:
    def __init__(self, inp: Input, endpoints: Dict[str, List[host.PartialEndpoint]]):
        self._input = inp
        self._endpoints = endpoints

    def update(self, endpoints: Dict[str, List[host.PartialEndpoint]]) -> None:
        self._endpoints = endpoints

    class Placement(NamedTuple):
        client: host.Endpoint
        leader: host.Endpoint
        replicas: List[host.Endpoint]

    def prom_placement(self) -> Placement:
        ports = itertools.count(30001, 100)

        def portify_one(e: host.PartialEndpoint) -> host.Endpoint:
            return host.Endpoint(e.host, next(ports) if self._input.monitored else -1)

        def portify(role: str, n: int) -> List[host.Endpoint]:
            assert n <= len(self._endpoints[role])
            return [portify_one(e) for e in self._endpoints[role][:n]]
        
        return self.Placement(
            client=portify_one(self._endpoints['clients'][0]),
            leader=portify_one(self._endpoints['leaders'][0]),
            replicas=portify('replicas', self._input.num_replicas),
        )

    def placement(self) -> Placement:
        ports = itertools.count(10000, 100)

        def portify_one(e: host.PartialEndpoint) -> host.Endpoint:
            if e.port is None:
                return host.Endpoint(e.host, next(ports))
            return e

        def portify(role: str, n: int) -> List[host.Endpoint]:
            assert n <= len(self._endpoints[role])
            return [portify_one(e) for e in self._endpoints[role][:n]]

        return self.Placement(
            client=portify_one(self._endpoints['clients'][0]),
            leader=portify_one(self._endpoints['leaders'][0]),
            replicas=portify('replicas', self._input.num_replicas),
        )

    def config(self) -> proto_util.Message:
        return {
            'replica_address': [{
                'host': e.host.ip(),
                'port': e.port,
            } for e in self.placement().replicas],
            'leader_address': {
                'host': self.placement().leader.host.ip(),
                'port': self.placement().leader.port,
            }
        }


# Suite ########################################################################
class VotingSuite(benchmark.Suite[Input, Output]):
    def run_benchmark(self, bench: benchmark.BenchmarkDirectory,
                      args: Dict[Any, Any], inp: Input) -> Output:
        net = VotingNet(inp, self.provisioner.hosts(1))

        if self.service_type("leaders") == "hydroflow":
            leader_proc = self.provisioner.popen_hydroflow(bench, 'leaders', 1, [
                '--service',
                'leader',
                '--prometheus-host',
                net.prom_placement().leader.host.ip(),
                '--prometheus-port',
                str(net.prom_placement().leader.port),
                '--flush-every-n',
                str(inp.leader_flush_every_n),
            ])
        
        if self.service_type("replicas") == "hydroflow":
            replica_procs: List[proc.Proc] = []
            for (i, replica) in enumerate(net.prom_placement().replicas):
                replica_procs.append(self.provisioner.popen_hydroflow(bench, f'replicas_{i}', 1,[
                    '--service',
                    'participant',
                    '--index',
                    str(i),
                    '--prometheus-host',
                    replica.host.ip(),
                    '--prometheus-port',
                    str(replica.port),
                ]))

        bench.log("Reconfiguring the system for a new benchmark")
        endpoints, receive_endpoints = self.provisioner.rebuild(1, {"clients": ["leaders"], "leaders": ["clients", "replicas"], "replicas": ["leaders"]})
        net.update(endpoints)
        bench.log("Reconfiguration completed")

        config = net.config()
        config_filename = bench.abspath('config.pbtxt')
        bench.write_string(config_filename,
                           proto_util.message_to_pbtext(config))
        bench.log('Config file config.pbtxt written.')

        # If we're monitoring the code, run garbage collection verbosely.
        java = ['java']
        if inp.monitored:
            java += [
                '-verbose:gc',
                '-XX:-PrintGC',
                '-XX:+PrintHeapAtGC',
                '-XX:+PrintGCDetails',
                '-XX:+PrintGCTimeStamps',
                '-XX:+PrintGCDateStamps',
            ]
        # Increase the heap size.
        java += [f'-Xms{inp.jvm_heap_size}', f'-Xmx{inp.jvm_heap_size}']

        # Launch leader.   
        if self.service_type("leaders") == "scala":
            leader_proc = bench.popen(
                host=net.placement().leader.host,
                label=f'leaders',
                cmd=java + [
                    '-cp',
                    os.path.abspath(args['jar']),
                    'frankenpaxos.voting.LeaderMain',
                    '--config',
                    config_filename,
                    '--prometheus_host',
                    net.prom_placement().leader.host.ip(),
                    '--prometheus_port',
                    str(net.prom_placement().leader.port),
                    '--log_level',
                    inp.log_level,
                ]
            )
            if inp.profiled:
                leader_proc = perf_util.JavaPerfProc(bench,
                                                    net.placement().leader.host,
                                                    leader_proc, f'leaders')
        bench.log('Leader started.')

        # Launch replicas
        if self.service_type("replicas") == "scala":
            replica_procs: List[proc.Proc] = []
            for (i, replica) in enumerate(net.placement().replicas):
                replica_proc = bench.popen(
                    host=replica.host,
                    label=f'replicas_{i}',
                    cmd=java + [
                        '-cp',
                        os.path.abspath(args['jar']),
                        'frankenpaxos.voting.ReplicaMain',
                        '--config',
                        config_filename,
                        '--index',
                        str(i),
                        '--prometheus_host',
                        net.prom_placement().replicas[i].host.ip(),
                        '--prometheus_port',
                        str(net.prom_placement().replicas[i].port),
                        '--log_level',
                        inp.log_level,
                    ],
                )
                if inp.profiled:
                    replica_proc = perf_util.JavaPerfProc(bench, replica.host,
                                                        replica_proc, f'replicas_{i}')
                replica_procs.append(replica_proc)
        bench.log('Replicas started.')

        # Launch Prometheus.
        if inp.monitored:
            prometheus_config = prometheus.prometheus_config(
                int(inp.prometheus_scrape_interval.total_seconds() * 1000), {
                    'voting_client': [
                        f'{net.prom_placement().client.host.ip()}:{net.prom_placement().client.port}'
                    ],
                    'voting_leader': [
                        f'{net.prom_placement().leader.host.ip()}:' +
                        f'{net.prom_placement().leader.port}'
                    ],
                    'voting_replica': [
                        f'{e.host.ip()}:{e.port}'
                        for e in net.prom_placement().replicas
                    ]
                })
            bench.write_string('prometheus.yml', yaml.dump(prometheus_config))
            prometheus_server = bench.popen(
                host=host.LocalHost(),
                label='prometheus',
                cmd=[
                    'prometheus',
                    f'--config.file={bench.abspath("prometheus.yml")}',
                    f'--storage.tsdb.path={bench.abspath("prometheus_data")}',
                    f'--web.listen-address=:0' # Arbitrary prometheus port to avoid conflicts
                ],
            )
            bench.log('Prometheus started.')

        time.sleep(inp.client_lag.total_seconds())
        bench.log('Client lag ended.')

        # Launch the client
        client = net.placement().client
        client_proc = bench.popen(
            host=client.host,
            label=f'clients',
            # TODO(mwhittaker): For now, we don't run clients with large
            # heaps and verbose garbage collection because they are all
            # colocated on one machine.
            cmd=[
                'java',
                '-cp',
                os.path.abspath(args['jar']),
                'frankenpaxos.voting.ClientMain',
                '--host',
                client.host.ip(),
                '--port',
                str(client.port),
                '--config',
                config_filename,
                '--duration',
                f'{inp.duration.total_seconds()}s',
                '--timeout',
                f'{inp.timeout.total_seconds()}s',
                '--num_clients',
                f'{inp.num_clients_per_proc}',
                '--num_warmup_clients',
                f'{inp.num_clients_per_proc}',
                '--warmup_duration',
                f'{inp.warmup_duration.total_seconds()}s',
                '--warmup_timeout',
                f'{inp.warmup_timeout.total_seconds()}s',
                '--warmup_sleep',
                f'{inp.warmup_sleep.total_seconds()}s',
                '--output_file',
                bench.abspath(f'client_data.csv'),
                '--prometheus_host',
                net.prom_placement().client.host.ip(),
                '--prometheus_port',
                str(net.prom_placement().client.port),
                '--log_level',
                inp.log_level,
                '--receive_addrs',
                ','.join([str(x) for x in receive_endpoints[0]]),
            ])
        if inp.profiled:
            client_proc = perf_util.JavaPerfProc(
                bench, client.host, client_proc, f'clients')

        bench.log(f'Clients started and running for {inp.duration}.')

        # Wait for the client to finish and then terminate the server.
        client_proc.wait()
        leader_proc.kill()
        for p in replica_procs:
             p.kill()
        if inp.monitored:
             prometheus_server.kill()

        bench.log('Clients finished and processes terminated.')

        client_csvs = [
            bench.abspath(f'client_data.csv')
        ]

        return benchmark.parse_recorder_data(
            bench,
            client_csvs,
            drop_prefix=datetime.timedelta(seconds=0),
            save_data=False)


def get_parser() -> argparse.ArgumentParser:
    return parser_util.get_benchmark_parser()