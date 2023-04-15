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
    persist_log: bool
    flush_every_n: int
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
    def __init__(self, inp: Input, endpoints: Dict[str, List[host.PartialEndpoint]]):
        self._input = inp
        self._endpoints = endpoints

    def update(self, endpoints: Dict[str, List[host.PartialEndpoint]]) -> None:
        self._endpoints = endpoints

    class Placement(NamedTuple):
        client: host.Endpoint
        server: host.Endpoint

    def prom_placement(self) -> Placement:
        ports = itertools.count(40001, 100)

        def portify_one(role: str, index: int) -> host.Endpoint:
            e = self._endpoints[role][index]

            return host.Endpoint(e.host, next(ports) if self._input.monitored else -1)

        return self.Placement(
            client=portify_one('clients', 0),
            server=portify_one('servers', 0),
        )

    def placement(self) -> Placement:
        ports = itertools.count(10000, 100)

        def portify_one(e: host.PartialEndpoint) -> host.Endpoint:
            if e.port is None:
                return host.Endpoint(e.host, next(ports))
            return e

        return self.Placement(
            client=portify_one(self._endpoints['clients'][0]),
            server=portify_one(self._endpoints['servers'][0]),
        )


# Suite ########################################################################
class EchoSuite(benchmark.Suite[Input, Output]):
    def run_benchmark(self, bench: benchmark.BenchmarkDirectory,
                      args: Dict[Any, Any], inp: Input) -> Output:
        net = EchoNet(inp, self.provisioner.hosts(1))

        if self.service_type("servers") == "hydroflow":
            server_proc = self.provisioner.popen_hydroflow(bench, 'servers', 1, [ 
                '--service',
                'server',
                '--prometheus-host',
                net.prom_placement().server.host.ip(),
                '--prometheus-port',
                str(net.prom_placement().server.port),
            ] +
            (['--persist-log'] if inp.persist_log else []))

        bench.log("Reconfiguring the system for a new benchmark")
        endpoints, receive_endpoints = self.provisioner.rebuild(1, {"clients": ["servers"], "servers": ["clients"]})
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

        # Launch server.   
        if self.service_type("servers") == "scala":
            server_proc = bench.popen(
                host=net.placement().server.host,
                label=f'servers',
                cmd=java + [
                    '-cp',
                    os.path.abspath(args['jar']),
                    'frankenpaxos.echo.BenchmarkServerMain',
                    '--host',
                    net.placement().server.host.ip(),
                    '--port',
                    str(net.placement().server.port),
                    '--persist_log',
                    'true' if inp.persist_log else 'false',
                    '--flush_every_n',
                    str(inp.flush_every_n),
                    '--prometheus_host',
                    net.prom_placement().server.host.ip(),
                    '--prometheus_port',
                    str(net.prom_placement().server.port),
                ],
            )
            if inp.profiled:
                server_proc = perf_util.JavaPerfProc(bench,
                                                    net.placement().server.host,
                                                    server_proc, f'servers')
        bench.log('Server started.')

        # Launch Prometheus.
        if inp.monitored:
            prometheus_config = prometheus.prometheus_config(
                int(inp.prometheus_scrape_interval.total_seconds() * 1000), {
                    'echo_client': [
                        f'{net.prom_placement().client.host.ip()}:{net.prom_placement().client.port}'
                    ],
                    'echo_server': [
                        f'{net.prom_placement().server.host.ip()}:' +
                        f'{net.prom_placement().server.port}'
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

        time.sleep(inp.client_lag.total_seconds())
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
                '--flush_every_n',
                str(inp.flush_every_n),
                '--prometheus_host',
                net.prom_placement().client.host.ip(),
                '--prometheus_port',
                str(net.prom_placement().client.port),
                '--receive_addrs',
                ','.join([str(x) for x in receive_endpoints[0]]),
            ])
        if inp.profiled:
            client_proc = perf_util.JavaPerfProc(
                bench, client.host, client_proc, f'client')

        bench.log(f'Clients started and running for {inp.duration}.')

        # Wait for the client to finish and then terminate the server.
        client_proc.wait()
        server_proc.kill()
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