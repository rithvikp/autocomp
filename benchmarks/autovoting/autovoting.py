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
from .. import read_write_workload
from .. import provision
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

# Input/Output #################################################################
class Input(NamedTuple):
    # System-wide parameters. ##################################################
    num_client_procs: int
    num_clients_per_proc: int
    num_replica_groups: int
    num_replica_partitions: int
    num_collectors: int
    num_broadcasters: int
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
    workload: read_write_workload.ReadWriteWorkload


Output = benchmark.RecorderOutput


# Networks #####################################################################
class AutoVotingNet:
    def __init__(self, inp: Input, endpoints: Dict[str, provision.EndpointProvider]):
        self._input = inp
        self._endpoints = endpoints

    def update(self, endpoints: Dict[str, provision.EndpointProvider]) -> None:
        self._endpoints = endpoints

    class Placement(NamedTuple):
        clients: List[host.Endpoint]
        leader: host.Endpoint
        collectors: List[host.Endpoint]
        broadcasters: List[host.Endpoint]
        replicas: List[host.Endpoint]

    def prom_placement(self) -> Placement:
        ports = itertools.count(40001, 100)

        def portify_one(e: host.PartialEndpoint) -> host.Endpoint:
            return host.Endpoint(e.host, next(ports) if self._input.monitored else -1)

        def portify(role: str, n: int) -> List[host.Endpoint]:
            return [portify_one(e) for e in self._endpoints[role].get_range(n, sender=-1)]
        
        return self.Placement(
            clients=portify('clients', self._input.num_client_procs),
            leader=portify_one(self._endpoints['leaders'].get(0, sender=-1)),
            collectors=portify('collectors', self._input.num_collectors),
            broadcasters=portify('broadcasters', self._input.num_broadcasters),
            replicas=portify('replicas', self._input.num_replica_groups * self._input.num_replica_partitions),
        )

    def placement(self, index: int = 0) -> Placement:
        ports = itertools.count(10000, 100)

        def portify_one(e: host.PartialEndpoint) -> host.Endpoint:
            if e.port is None:
                return host.Endpoint(e.host, next(ports))
            return e

        def portify(role: str, n: int) -> List[host.Endpoint]:
            return [portify_one(e) for e in self._endpoints[role].get_range(n, sender=index)]

        return self.Placement(
            clients=portify('clients', self._input.num_client_procs),
            leader=portify_one(self._endpoints['leaders'].get(0, sender=index)),
            collectors=portify('collectors', self._input.num_collectors),
            broadcasters=portify('broadcasters', self._input.num_broadcasters),
            replicas=portify('replicas', self._input.num_replica_groups * self._input.num_replica_partitions),
        )

    def config(self, index: int = 0) -> proto_util.Message:
        return {
            'replica_address': [{
                'host': e.host.ip(),
                'port': e.port,
            } for e in self.placement(index=index).replicas],
            'leader_address': {
                'host': self.placement(index=index).leader.host.ip(),
                'port': self.placement(index=index).leader.port,
            }
        }


# Suite ########################################################################
class AutoVotingSuite(benchmark.Suite[Input, Output]):
    def run_benchmark(self, bench: benchmark.BenchmarkDirectory,
                      args: Dict[Any, Any], inp: Input) -> Output:
        net = AutoVotingNet(inp, self.provisioner.hosts(1))

        if self.service_type("leaders") == "hydroflow":
            leader_proc = self.provisioner.popen_hydroflow(bench, 'leaders', 1, [
                '--service',
                'leader',
                '--prometheus-host',
                net.prom_placement().leader.host.ip(),
                f'--prometheus-port={str(net.prom_placement().leader.port)}',
                '--flush-every-n',
                str(inp.leader_flush_every_n),
            ])
        else:
            raise ValueError("AutoVoting only currently supports hydroflow leaders")

        if self.service_type("collectors") == "hydroflow":
            collector_procs: List[proc.Proc] = []
            for (i, collector) in enumerate(net.prom_placement().collectors):
                collector_procs.append(self.provisioner.popen_hydroflow(bench, f'collectors_{i}', 1, [
                    '--service',
                    'collector',
                    '--prometheus-host',
                    collector.host.ip(),
                    f'--prometheus-port={str(collector.port)}',
                    '--collector.num-replica-groups',
                    str(inp.num_replica_groups),
                    '--collector.num-replica-partitions',
                    str(inp.num_replica_partitions),
                ]))
        else:
            raise ValueError("AutoVoting only currently supports hydroflow collectors")
        
        if self.service_type("broadcasters") == "hydroflow":
            broadcaster_procs: List[proc.Proc] = []
            for (i, broadcaster) in enumerate(net.prom_placement().broadcasters):
                broadcaster_procs.append(self.provisioner.popen_hydroflow(bench, f'broadcasters_{i}', 1, [
                    '--service',
                    'broadcaster',
                    '--prometheus-host',
                    broadcaster.host.ip(),
                    f'--prometheus-port={str(broadcaster.port)}',
                    '--broadcaster.num-replica-groups',
                    str(inp.num_replica_groups),
                    '--broadcaster.num-replica-partitions',
                    str(inp.num_replica_partitions),
                ]))
        else:
            raise ValueError("AutoVoting only currently supports hydroflow broadcasters")
        
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
                    f'--prometheus-port={str(replica.port)}',
                ]))
        else:
            raise ValueError("AutoVoting only currently supports hydroflow replicas")

        bench.log("Reconfiguring the system for a new benchmark")
        endpoints, receive_endpoints = self.provisioner.rebuild(1, {
            "clients": ["leaders"],
            "leaders": ["broadcasters"],
            "broadcasters": ["replicas"],
            "replicas": ["collectors"],
            "collectors": ["clients"],
        },
        {
            "clients": inp.num_client_procs,
        })
        net.update(endpoints)
        bench.log("Reconfiguration completed")

        config = net.config()
        config_filename = bench.abspath('config.pbtxt')
        bench.write_string(config_filename,
                           proto_util.message_to_pbtext(config))
        bench.log('Config file config.pbtxt written.')

        client_config_filenames = []
        for i in range(inp.num_client_procs):
            filename = bench.abspath(f"client_config_{i}.pbtxt")
            bench.write_string(filename,
                           proto_util.message_to_pbtext(net.config(index=i)))
            client_config_filenames.append(filename)

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

        # Launch Prometheus.
        if inp.monitored:
            prometheus_config = prometheus.prometheus_config(
                int(inp.prometheus_scrape_interval.total_seconds() * 1000), {
                    'voting_client': [
                        f'{e.host.ip()}:{e.port}'
                        for e in net.prom_placement().clients
                    ],
                    'voting_leader': [
                        f'{net.prom_placement().leader.host.ip()}:' +
                        f'{net.prom_placement().leader.port}'
                    ],
                    'voting_collector': [
                        f'{e.host.ip()}:{e.port}'
                        for e in net.prom_placement().collectors
                    ],
                    'voting_broadcaster': [
                        f'{e.host.ip()}:{e.port}'
                        for e in net.prom_placement().broadcasters
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

        # Lag clients.
        time.sleep(inp.client_lag.total_seconds())
        bench.log('Client lag ended.')

        # Launch clients.
        workload_filename = bench.abspath('workload.pbtxt')
        bench.write_string(
            workload_filename,
            proto_util.message_to_pbtext(inp.workload.to_proto()))
 
        client_procs: List[proc.Proc] = []
        for i in range(inp.num_client_procs):
            client = net.placement(index=i).clients[i]
            client_proc = bench.popen(
                host=client.host,
                label=f'clients_{i}',
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
                    client_config_filenames[i],
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
                    bench.abspath(f'client_{i}_data.csv'),
                    '--prometheus_host',
                    net.prom_placement().clients[i].host.ip(),
                    '--prometheus_port',
                    str(net.prom_placement().clients[i].port),
                    '--log_level',
                    inp.log_level,
                    '--receive_addrs',
                    ','.join([str(x) for x in receive_endpoints[i]]),
                    '--workload',
                    f'{workload_filename}',
                ])
            if inp.profiled:
                client_proc = perf_util.JavaPerfProc(
                    bench, client.host, client_proc, f'clients')
            client_procs.append(client_proc)

        bench.log(f'Clients started and running for {inp.duration}.')

        # Wait for the clients to finish and then terminate the server.
        for p in client_procs:
            p.wait()

        leader_proc.kill()
        for p in (replica_procs+collector_procs+broadcaster_procs):
             p.kill()
        if inp.monitored:
             prometheus_server.kill()

        bench.log('Clients finished and processes terminated.')

        client_csvs = [
            bench.abspath(f'client_{i}_data.csv')
            for i in range(inp.num_client_procs)
        ]

        return benchmark.parse_recorder_data(
            bench,
            client_csvs,
            drop_prefix=datetime.timedelta(seconds=0),
            save_data=False)


def get_parser() -> argparse.ArgumentParser:
    return parser_util.get_benchmark_parser()