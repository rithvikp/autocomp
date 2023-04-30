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
from typing import Any, Callable, Collection, Dict, List, NamedTuple, Optional, Tuple
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

class MicrobenchmarkType(enum.Enum):
    DECOUPLING_FUNCTIONAL = enum.auto()
    DECOUPLING_MONOTONIC = enum.auto()
    DECOUPLING_MUTUALLY_INDEPENDENT = enum.auto()
    DECOUPLING_STATE_MACHINE = enum.auto()
    DECOUPLING_GENERAL = enum.auto()
    PARTITIONING_DEPENDENCIES = enum.auto()
    PARTITIONING_PARTIAL = enum.auto()

    AUTO_DECOUPLING_FUNCTIONAL = enum.auto()
    AUTO_DECOUPLING_MONOTONIC = enum.auto()
    AUTO_DECOUPLING_MUTUALLY_INDEPENDENT = enum.auto()
    AUTO_DECOUPLING_STATE_MACHINE = enum.auto()
    AUTO_DECOUPLING_GENERAL = enum.auto()
    AUTO_PARTITIONING_DEPENDENCIES = enum.auto()
    AUTO_PARTITIONING_PARTIAL = enum.auto()


class DecouplingMonotonicOptions(NamedTuple):
    num_replicas: int

class DecouplingMutuallyIndependentOptions(NamedTuple):
    num_replicas: int

class PartitioningDependenciesOptions(NamedTuple):
    num_replicas: int

class PartitioningPartialOptions(NamedTuple):
    num_replicas: int

# Input/Output #################################################################
class Input(NamedTuple):
    # System-wide parameters. ##################################################
    num_client_procs: int
    num_clients_per_proc: int
    jvm_heap_size: str
    log_level: str

    # Benchmark parameters. ####################################################
    microbenchmark_type: MicrobenchmarkType
    duration: datetime.timedelta
    timeout: datetime.timedelta
    warmup_duration: datetime.timedelta
    warmup_timeout: datetime.timedelta
    warmup_sleep: datetime.timedelta
    client_lag: datetime.timedelta
    profiled: bool
    monitored: bool
    prometheus_scrape_interval: datetime.timedelta

    decoupling_monotonic_options: DecouplingMonotonicOptions = None
    decoupling_mutually_independent_options: DecouplingMutuallyIndependentOptions = None
    partitioning_dependencies_options: PartitioningDependenciesOptions = None
    partitioning_partial_options: PartitioningPartialOptions = None


Output = benchmark.RecorderOutput


#AutoMicrobenchmarksNetworks #####################################################################
class AutoMicrobenchmarksNet:
    def __init__(self, inp: Input, endpoints: Dict[str, provision.EndpointProvider]):
        self._input = inp
        self._endpoints = endpoints

    def update(self, endpoints: Dict[str, provision.EndpointProvider]) -> None:
        self._endpoints = endpoints

    class Placement(NamedTuple):
        clients: List[host.Endpoint]
        leader: host.Endpoint

    def prom_placement(self) -> Placement:
        ports = itertools.count(30001, 100)

        def portify_one(e: host.PartialEndpoint) -> host.Endpoint:
            return host.Endpoint(e.host, next(ports) if self._input.monitored else -1)

        def portify(role: str, n: int) -> List[host.Endpoint]:
            return [portify_one(e) for e in self._endpoints[role].get_range(n, sender=-1)]
        
        return self.Placement(
            clients=portify('clients', self._input.num_client_procs),
            leader=portify_one(self._endpoints['leaders'].get(0, sender=-1)),
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
        )

# Suite ########################################################################
class AutoMicrobenchmarksSuite(benchmark.Suite[Input, Output]):
    # To add a new microbenchmark, add a new function following the form of _decoupling_functional,
    # and add a new case to the if-else in run_benchmark. Non-leader/client services cannot be monitored.
    # Add a new options namedtuple if necessary.
    #
    # Make sure to also update dedalus_.py to run the new microbenchmark.


    def _decoupling_functional(self, bench: benchmark.BenchmarkDirectory, args: Dict[Any, Any], inp: Input, net:AutoMicrobenchmarksNet) -> Tuple[Dict[str, provision.EndpointProvider], List[List[host.Endpoint]], Dict[str, List[str]], proc.Proc, List[proc.Proc]]:
        leader_proc = self.provisioner.popen_hydroflow(bench, 'leaders', 1, [ '--service',
            'leader',
            '--prometheus-host',
            net.prom_placement().leader.host.ip(),
            f'--prometheus-port={str(net.prom_placement().leader.port)}',
        ],
        example="decoupling_functional")
        
        endpoints, receive_endpoints = self.provisioner.rebuild(1, {"clients": ["leaders"], "leaders": ["clients"]}, {"clients": inp.num_client_procs})

        prom_endpoints = {
            'automicrobenchmarks_leader': [
                f'{net.prom_placement().leader.host.ip()}:' +
                f'{net.prom_placement().leader.port}'
            ],
        }
        
        return endpoints, receive_endpoints, prom_endpoints, leader_proc, []

    def _decoupling_monotonic(self, bench: benchmark.BenchmarkDirectory, args: Dict[Any, Any], inp: Input, net: AutoMicrobenchmarksNet) -> Tuple[Dict[str, provision.EndpointProvider], List[List[host.Endpoint]], Dict[str, List[str]], proc.Proc, List[proc.Proc]]:
        leader_proc = self.provisioner.popen_hydroflow(bench, 'leaders', 1, [
            '--service',
            'leader',
            '--prometheus-host',
            net.prom_placement().leader.host.ip(),
            f'--prometheus-port={str(net.prom_placement().leader.port)}',
        ],
        example="decoupling_monotonic")

        replica_procs: List[proc.Proc] = []
        for i in range(inp.decoupling_monotonic_options.num_replicas):
            replica_procs.append(self.provisioner.popen_hydroflow(bench, f'replicas_{i}', 1,[
                '--service',
                'replica',
                '--index',
                str(i),
            ],
            example="decoupling_monotonic"))
        
        endpoints, receive_endpoints = self.provisioner.rebuild(1, {
            "clients": ["leaders"],
            "leaders": ["clients", "replicas"],
            "replicas": ["leaders"],
        }, {"clients": inp.num_client_procs})

        prom_endpoints = {
            'automicrobenchmarks_leader': [
                f'{net.prom_placement().leader.host.ip()}:' +
                f'{net.prom_placement().leader.port}'
            ],
        }
        
        return endpoints, receive_endpoints, prom_endpoints, leader_proc, replica_procs

    def _decoupling_mutually_independent(self, bench: benchmark.BenchmarkDirectory, args: Dict[Any, Any], inp: Input, net: AutoMicrobenchmarksNet) -> Tuple[Dict[str, provision.EndpointProvider], List[List[host.Endpoint]], Dict[str, List[str]], proc.Proc, List[proc.Proc]]:
        leader_proc = self.provisioner.popen_hydroflow(bench, 'leaders', 1, [
            '--service',
            'leader',
            '--prometheus-host',
            net.prom_placement().leader.host.ip(),
            f'--prometheus-port={str(net.prom_placement().leader.port)}',
        ],
        example="decoupling_mutually_independent")

        replica_procs: List[proc.Proc] = []
        for i in range(inp.decoupling_mutually_independent_options.num_replicas):
            replica_procs.append(self.provisioner.popen_hydroflow(bench, f'replicas_{i}', 1,[
                '--service',
                'replica',
                '--index',
                str(i),
            ],
            example="decoupling_mutually_independent"))
        
        endpoints, receive_endpoints = self.provisioner.rebuild(1, {
            "clients": ["leaders"],
            "leaders": ["clients", "replicas"],
            "replicas": ["leaders"],
        }, {"clients": inp.num_client_procs})

        prom_endpoints = {
            'automicrobenchmarks_leader': [
                f'{net.prom_placement().leader.host.ip()}:' +
                f'{net.prom_placement().leader.port}'
            ],
        }
        
        return endpoints, receive_endpoints, prom_endpoints, leader_proc, replica_procs

    def _decoupling_state_machine(self, bench: benchmark.BenchmarkDirectory, args: Dict[Any, Any], inp: Input, net: AutoMicrobenchmarksNet) -> Tuple[Dict[str, provision.EndpointProvider], List[List[host.Endpoint]], Dict[str, List[str]], proc.Proc, List[proc.Proc]]:
        leader_proc = self.provisioner.popen_hydroflow(bench, 'leaders', 1, [ '--service',
            'leader',
            '--prometheus-host',
            net.prom_placement().leader.host.ip(),
            f'--prometheus-port={str(net.prom_placement().leader.port)}',
        ],
        example="decoupling_state_machine")
        
        endpoints, receive_endpoints = self.provisioner.rebuild(1, {"clients": ["leaders"], "leaders": ["clients"]}, {"clients": inp.num_client_procs})

        prom_endpoints = {
            'automicrobenchmarks_leader': [
                f'{net.prom_placement().leader.host.ip()}:' +
                f'{net.prom_placement().leader.port}'
            ],
        }
        
        return endpoints, receive_endpoints, prom_endpoints, leader_proc, []

    def _decoupling_general(self, bench: benchmark.BenchmarkDirectory, args: Dict[Any, Any], inp: Input, net: AutoMicrobenchmarksNet) -> Tuple[Dict[str, provision.EndpointProvider], List[List[host.Endpoint]], Dict[str, List[str]], proc.Proc, List[proc.Proc]]:
        leader_proc = self.provisioner.popen_hydroflow(bench, 'leaders', 1, [ '--service',
            'leader',
            '--prometheus-host',
            net.prom_placement().leader.host.ip(),
            f'--prometheus-port={str(net.prom_placement().leader.port)}',
        ],
        example="decoupling_general")
        
        endpoints, receive_endpoints = self.provisioner.rebuild(1, {"clients": ["leaders"], "leaders": ["clients"]}, {"clients": inp.num_client_procs})

        prom_endpoints = {
            'automicrobenchmarks_leader': [
                f'{net.prom_placement().leader.host.ip()}:' +
                f'{net.prom_placement().leader.port}'
            ],
        }
        
        return endpoints, receive_endpoints, prom_endpoints, leader_proc, []

    def _partitioning_dependencies(self, bench: benchmark.BenchmarkDirectory, args: Dict[Any, Any], inp: Input, net: AutoMicrobenchmarksNet) -> Tuple[Dict[str, provision.EndpointProvider], List[List[host.Endpoint]], Dict[str, List[str]], proc.Proc, List[proc.Proc]]:
        leader_proc = self.provisioner.popen_hydroflow(bench, 'leaders', 1, [
            '--service',
            'leader',
            '--prometheus-host',
            net.prom_placement().leader.host.ip(),
            f'--prometheus-port={str(net.prom_placement().leader.port)}',
        ],
        example="partitioning_dependencies")

        replica_procs: List[proc.Proc] = []
        for i in range(inp.partitioning_dependencies_options.num_replicas):
            replica_procs.append(self.provisioner.popen_hydroflow(bench, f'replicas_{i}', 1,[
                '--service',
                'replica',
                '--index',
                str(i),
            ],
            example="partitioning_dependencies"))
        
        endpoints, receive_endpoints = self.provisioner.rebuild(1, {
            "clients": ["leaders"],
            "leaders": ["clients", "replicas"],
            "replicas": ["leaders"],
        }, {"clients": inp.num_client_procs})

        prom_endpoints = {
            'automicrobenchmarks_leader': [
                f'{net.prom_placement().leader.host.ip()}:' +
                f'{net.prom_placement().leader.port}'
            ],
        }
        
        return endpoints, receive_endpoints, prom_endpoints, leader_proc, replica_procs

    def _partitioning_partial(self, bench: benchmark.BenchmarkDirectory, args: Dict[Any, Any], inp: Input, net: AutoMicrobenchmarksNet) -> Tuple[Dict[str, provision.EndpointProvider], List[List[host.Endpoint]], Dict[str, List[str]], proc.Proc, List[proc.Proc]]:
        leader_proc = self.provisioner.popen_hydroflow(bench, 'leaders', 1, [
            '--service',
            'leader',
            '--prometheus-host',
            net.prom_placement().leader.host.ip(),
            f'--prometheus-port={str(net.prom_placement().leader.port)}',
        ],
        example="partitioning_partial")

        replica_procs: List[proc.Proc] = []
        for i in range(inp.partitioning_partial_options.num_replicas):
            replica_procs.append(self.provisioner.popen_hydroflow(bench, f'replicas_{i}', 1,[
                '--service',
                'replica',
                '--index',
                str(i),
            ],
            example="partitioning_partial"))
        
        endpoints, receive_endpoints = self.provisioner.rebuild(1, {
            "clients": ["leaders"],
            "leaders": ["clients", "replicas", "replicas"],
            "replicas": ["leaders"],
        }, {"clients": inp.num_client_procs})

        prom_endpoints = {
            'automicrobenchmarks_leader': [
                f'{net.prom_placement().leader.host.ip()}:' +
                f'{net.prom_placement().leader.port}'
            ],
        }
        
        return endpoints, receive_endpoints, prom_endpoints, leader_proc, replica_procs

    def _auto_decoupling_functional(self, bench: benchmark.BenchmarkDirectory, args: Dict[Any, Any], inp: Input, net: AutoMicrobenchmarksNet) -> Tuple[Dict[str, provision.EndpointProvider], List[List[host.Endpoint]], Dict[str, List[str]], proc.Proc, List[proc.Proc]]:
        leader_proc = self.provisioner.popen_hydroflow(bench, 'leaders', 1, [
            '--service',
            'leader',
            '--prometheus-host',
            net.prom_placement().leader.host.ip(),
            f'--prometheus-port={str(net.prom_placement().leader.port)}',
        ],
        example="auto_decoupling_functional")

        responder_proc = self.provisioner.popen_hydroflow(bench, f'responders', 1,[
            '--service',
            'responder',
        ],
        example="auto_decoupling_functional")
    
        endpoints, receive_endpoints = self.provisioner.rebuild(1, {
            "clients": ["leaders"],
            "leaders": ["responders"],
            "responders": ["clients"],
        }, {"clients": inp.num_client_procs})

        prom_endpoints = {
            'automicrobenchmarks_leader': [
                f'{net.prom_placement().leader.host.ip()}:' +
                f'{net.prom_placement().leader.port}'
            ],
        }
        
        return endpoints, receive_endpoints, prom_endpoints, leader_proc, [responder_proc]

    def _auto_decoupling_monotonic(self, bench: benchmark.BenchmarkDirectory, args: Dict[Any, Any], inp: Input, net: AutoMicrobenchmarksNet) -> Tuple[Dict[str, provision.EndpointProvider], List[List[host.Endpoint]], Dict[str, List[str]], proc.Proc, List[proc.Proc]]:
        leader_proc = self.provisioner.popen_hydroflow(bench, 'leaders', 1, [
            '--service',
            'leader',
            '--prometheus-host',
            net.prom_placement().leader.host.ip(),
            f'--prometheus-port={str(net.prom_placement().leader.port)}',
        ],
        example="auto_decoupling_monotonic")

        replica_procs: List[proc.Proc] = []
        for i in range(inp.decoupling_monotonic_options.num_replicas):
            replica_procs.append(self.provisioner.popen_hydroflow(bench, f'replicas_{i}', 1,[
                '--service',
                'replica',
                '--index',
                str(i),
            ],
            example="auto_decoupling_monotonic"))

        collector_proc = self.provisioner.popen_hydroflow(bench, f'collectors', 1,[
            '--service',
            'collector',
            '--collector.num-replicas',
            str(inp.decoupling_monotonic_options.num_replicas),
        ],
        example="auto_decoupling_monotonic")
        
        endpoints, receive_endpoints = self.provisioner.rebuild(1, {
            "clients": ["leaders"],
            "leaders": ["replicas", "collectors"],
            "replicas": ["collectors"],
            "collectors": ["clients"],
        }, {"clients": inp.num_client_procs})

        prom_endpoints = {
            'automicrobenchmarks_leader': [
                f'{net.prom_placement().leader.host.ip()}:' +
                f'{net.prom_placement().leader.port}'
            ],
        }
        
        return endpoints, receive_endpoints, prom_endpoints, leader_proc, replica_procs + [collector_proc]

    def _auto_decoupling_mutually_independent(self, bench: benchmark.BenchmarkDirectory, args: Dict[Any, Any], inp: Input, net: AutoMicrobenchmarksNet) -> Tuple[Dict[str, provision.EndpointProvider], List[List[host.Endpoint]], Dict[str, List[str]], proc.Proc, List[proc.Proc]]:
        leader_proc = self.provisioner.popen_hydroflow(bench, 'leaders', 1, [
            '--service',
            'leader',
            '--prometheus-host',
            net.prom_placement().leader.host.ip(),
            f'--prometheus-port={str(net.prom_placement().leader.port)}',
        ],
        example="auto_decoupling_mutually_independent")

        replica_procs: List[proc.Proc] = []
        for i in range(inp.decoupling_mutually_independent_options.num_replicas):
            replica_procs.append(self.provisioner.popen_hydroflow(bench, f'replicas_{i}', 1,[
                '--service',
                'replica',
                '--index',
                str(i),
            ],
            example="auto_decoupling_mutually_independent"))

        collector_proc = self.provisioner.popen_hydroflow(bench, f'collectors', 1,[
            '--service',
            'collector',
            '--collector.num-replicas',
            str(inp.decoupling_mutually_independent_options.num_replicas),
        ],
        example="auto_decoupling_mutually_independent")
        
        endpoints, receive_endpoints = self.provisioner.rebuild(1, {
            "clients": ["leaders"],
            "leaders": ["replicas"],
            "replicas": ["collectors"],
            "collectors": ["clients"],
        }, {"clients": inp.num_client_procs})

        prom_endpoints = {
            'automicrobenchmarks_leader': [
                f'{net.prom_placement().leader.host.ip()}:' +
                f'{net.prom_placement().leader.port}'
            ],
        }
        
        return endpoints, receive_endpoints, prom_endpoints, leader_proc, replica_procs + [collector_proc]

    def run_benchmark(self, bench: benchmark.BenchmarkDirectory,
                      args: Dict[Any, Any], inp: Input) -> Output:
        net = AutoMicrobenchmarksNet(inp, self.provisioner.hosts(1))
        bench.log("Reconfiguring the system for a new benchmark")

        fn = None
        if inp.microbenchmark_type == MicrobenchmarkType.DECOUPLING_FUNCTIONAL:
            fn = self._decoupling_functional
        elif inp.microbenchmark_type == MicrobenchmarkType.DECOUPLING_MONOTONIC:
            fn = self._decoupling_monotonic
        elif inp.microbenchmark_type == MicrobenchmarkType.DECOUPLING_MUTUALLY_INDEPENDENT:
            fn = self._decoupling_mutually_independent
        elif inp.microbenchmark_type == MicrobenchmarkType.DECOUPLING_STATE_MACHINE:
            fn = self._decoupling_state_machine
        elif inp.microbenchmark_type == MicrobenchmarkType.DECOUPLING_GENERAL:
            fn = self._decoupling_general
        elif inp.microbenchmark_type == MicrobenchmarkType.PARTITIONING_DEPENDENCIES:
            fn = self._partitioning_dependencies
        elif inp.microbenchmark_type == MicrobenchmarkType.PARTITIONING_PARTIAL:
            fn = self._partitioning_partial
        elif inp.microbenchmark_type == MicrobenchmarkType.AUTO_DECOUPLING_FUNCTIONAL:
            fn = self._auto_decoupling_functional
        elif inp.microbenchmark_type == MicrobenchmarkType.AUTO_DECOUPLING_MONOTONIC:
            fn = self._auto_decoupling_monotonic
        elif inp.microbenchmark_type == MicrobenchmarkType.AUTO_DECOUPLING_MUTUALLY_INDEPENDENT:
            fn = self._auto_decoupling_mutually_independent
        elif inp.microbenchmark_type == MicrobenchmarkType.AUTO_DECOUPLING_STATE_MACHINE:
            fn = self._auto_decoupling_state_machine
        elif inp.microbenchmark_type == MicrobenchmarkType.AUTO_DECOUPLING_GENERAL:
            fn = self._auto_decoupling_general
        elif inp.microbenchmark_type == MicrobenchmarkType.AUTO_PARTITIONING_DEPENDENCIES:
            fn = self._auto_partitioning_dependencies
        elif inp.microbenchmark_type == MicrobenchmarkType.AUTO_PARTITIONING_PARTIAL:
            fn = self._auto_partitioning_partial
        else:
            raise NotImplementedError(f"Microbenchmark type {inp.microbenchmark_type} is not implemented")

        endpoints, receive_endpoints, prom_endpoints, leader_proc, other_procs = fn(bench, args, inp, net)
        net.update(endpoints)
        bench.log("Reconfiguration completed")

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
                int(inp.prometheus_scrape_interval.total_seconds() * 1000), prom_endpoints.update({
                    'automicrobenchmarks_client': [
                        f'{e.host.ip()}:{e.port}'
                        for e in net.prom_placement().clients
                    ],
                }))
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
                    'frankenpaxos.automicrobenchmarks.ClientMain',
                    '--host',
                    client.host.ip(),
                    '--port',
                    str(client.port),
                    '--leader_host',
                    net.placement(index=i).leader.host.ip(),
                    '--leader_port',
                    str(net.placement(index=i).leader.port),
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
                ])
            if inp.profiled:
                client_proc = perf_util.JavaPerfProc(
                    bench, client.host, client_proc, f'clients_{i}')
            client_procs.append(client_proc)

        bench.log(f'Clients started and running for {inp.duration}.')

        # Wait for the clients to finish and then terminate the server.
        for p in client_procs:
            p.wait()

        leader_proc.kill()
        for p in other_procs:
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