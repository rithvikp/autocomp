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


# Input/Output #################################################################
class Input(NamedTuple):
    # System-wide parameters. ##################################################
    num_clients_per_proc: int
    jvm_heap_size: str

    # Benchmark parameters. ####################################################
    duration: datetime.timedelta
    timeout: datetime.timedelta
    warmup_duration: datetime.timedelta
    warmup_timeout: datetime.timedelta
    warmup_sleep: datetime.timedelta
    client_lag: datetime.timedelta
    profiled: bool
    monitored: bool
    prometheus_scrape_interval: datetime.timedelta


Output = benchmark.RecorderOutput


# Networks #####################################################################
class EchoNet:
    def __init__(self, cluster: cluster.Cluster, input: Input) -> None:
        self._cluster = cluster.f(1)
        self._input = input

    class Placement(NamedTuple):
        client: host.Endpoint
        server: host.Endpoint

    def placement(self) -> Placement:
        ports = itertools.count(10000, 100)

        def portify_one(h: host.Host) -> host.Endpoint:
            return host.Endpoint(h, next(ports))

        return self.Placement(
            client=portify_one(self._cluster['client'][0]),
            server=portify_one(self._cluster['server'][0]),
        )


# Suite ########################################################################
class EchoSuite(benchmark.Suite[Input, Output]):
    def __init__(self) -> None:
        super().__init__()
        self._cluster = cluster.Cluster.from_json_file(self.args()['cluster'],
                                                       self._connect)

    def _connect(self, address: str) -> host.Host:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.client.AutoAddPolicy)
        if self.args()['identity_file']:
            client.connect(address, key_filename=self.args()['identity_file'])
        else:
            client.connect(address)
        return host.RemoteHost(client)

    def run_benchmark(self, bench: benchmark.BenchmarkDirectory,
                      args: Dict[Any, Any], input: Input) -> Output:
        net = EchoNet(self._cluster, input)

        # If we're monitoring the code, run garbage collection verbosely.
        java = ['java']
        if input.monitored:
            java += [
                '-verbose:gc',
                '-XX:-PrintGC',
                '-XX:+PrintHeapAtGC',
                '-XX:+PrintGCDetails',
                '-XX:+PrintGCTimeStamps',
                '-XX:+PrintGCDateStamps',
            ]
        # Increase the heap size.
        java += [f'-Xms{input.jvm_heap_size}', f'-Xmx{input.jvm_heap_size}']

        # Launch server.
        server_proc = bench.popen(
            host=net.placement().server.host,
            label=f'server',
            cmd=java + [
                '-cp',
                os.path.abspath(args['jar']),
                'frankenpaxos.echo.BenchmarkServerMain',
                '--host',
                net.placement().server.host.ip(),
                '--port',
                str(net.placement().server.port),
                '--prometheus_host',
                net.placement().server.host.ip(),
                '--prometheus_port',
                str(net.placement().server.port +
                    1) if input.monitored else '-1',
            ],
        )
        if input.profiled:
            server_proc = perf_util.JavaPerfProc(bench,
                                                 net.placement().server.host,
                                                 server_proc, f'server')
        bench.log('Servers started.')

        # Launch Prometheus.
        if input.monitored:
            prometheus_config = prometheus.prometheus_config(
                int(input.prometheus_scrape_interval.total_seconds() * 1000), {
                    'echo_client': [
                        f'{net.placement().client.host.ip()}:{net.placement().client.port+1}'
                    ],
                    'echo_server': [
                        f'{net.placement().server.host.ip()}:' +
                        f'{net.placement().server.port+1}'
                    ],
                })
            bench.write_string('prometheus.yml', yaml.dump(prometheus_config))
            prometheus_server = bench.popen(
                host=host.LocalHost(),
                label='prometheus',
                cmd=[
                    'prometheus',
                    f'--config.file={bench.abspath("prometheus.yml")}',
                    f'--storage.tsdb.path={bench.abspath("prometheus_data")}',
                ],
            )
            bench.log('Prometheus started.')

        # Lag the client
        time.sleep(input.client_lag.total_seconds())
        bench.log('Client lag ended.')

        # Launch the client
        client = net.placement().client
        client_proc = bench.popen(
            host=client.host,
            label=f'client',
            # TODO(mwhittaker): For now, we don't run clients with large
            # heaps and verbose garbage collection because they are all
            # colocated on one machine.
            cmd=[
                'java',
                '-cp',
                os.path.abspath(args['jar']),
                'frankenpaxos.echo.BenchmarkClientMain',
                '--host',
                client.host.ip(),
                '--port',
                str(client.port),
                '--server_host',
                net.placement().server.host.ip(),
                '--server_port',
                str(net.placement().server.port),
                '--duration',
                f'{input.duration.total_seconds()}s',
                '--timeout',
                f'{input.timeout.total_seconds()}s',
                '--num_clients',
                f'{input.num_clients_per_proc}',
                '--num_warmup_clients',
                f'{input.num_clients_per_proc}',
                '--warmup_duration',
                f'{input.warmup_duration.total_seconds()}s',
                '--warmup_timeout',
                f'{input.warmup_timeout.total_seconds()}s',
                '--warmup_sleep',
                f'{input.warmup_sleep.total_seconds()}s',
                '--output_file',
                bench.abspath(f'client_data.csv'),
            ])
        if input.profiled:
            client_proc = perf_util.JavaPerfProc(
                bench, client.host, client_proc, f'client')

        bench.log(f'Clients started and running for {input.duration}.')

        # Wait for the client to finish and then terminate the server.
        client_proc.wait()
        server_proc.kill()
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
