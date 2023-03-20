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
    persistLog: bool
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

# Suite ########################################################################
class EchoSuite(benchmark.Suite[Input, Output]):
    def __init__(self) -> None:
        super().__init__()

    def config(self, endpoints: Dict[str, List[host.Endpoint]]) -> proto_util.Message:
        return {
            'f': self._input.f,
            'client_address': [{'host': e.host.ip(), 'port': e.port} for e in endpoints['clients']],
            'server_address': [{'host': e.host.ip(), 'port': e.port} for e in endpoints['clients']],
        }

    def run_benchmark(self, bench: benchmark.BenchmarkDirectory,
                      args: Dict[Any, Any], input: Input) -> Output:

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
        shared_server_args = lambda prom_port: [
            '--persist_log',
            'true' if input.persistLog else 'false',
            '--prometheus_host',
            '0.0.0.0',
            '--prometheus_port',
            str(prom_port) if input.monitored else '-1',
        ]
        if self.service_type("servers") == "hydro":
            server_proc = self.provisioner.popen_hydroflow(bench, f'server', 1, shared_server_args)
        else:
            server_proc = self.provisioner.popen(
                bench,
                label=f'client',
                f=1,
                cmd=lambda prom_port: java + [
                    '-cp',
                    os.path.abspath(args['jar']),
                    'frankenpaxos.echo.BenchmarkServerMain',
                ] + shared_server_args(prom_port),
            )
            if input.profiled:
                server_proc = perf_util.JavaPerfProc(bench,
                                                    net.placement().server.host,
                                                    server_proc, f'server')
        bench.log('Server started.')

        # Launch Prometheus.

        def write_configs(endpoints: Dict[str, List[host.Endpoint]], prometheus_endpoints: Dict[str, List[host.Endpoint]]) -> None:
            # Write the config.
            config = self.config(endpoints)
            bench.write_string('config.pbtxt', proto_util.message_to_pbtext(config))

            # Setup prometheus
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

            bench.log('Finished writing out config files post-deployment')

        # Lag the client
        self.provisioner.rebuild({"clients": "servers", "servers": "clients"})

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
                '--prometheus_host',
                client.host.ip(),
                '--prometheus_port',
                str(client.port+1) if input.monitored else '-1',
            ])
        if input.profiled:
            client_proc = perf_util.JavaPerfProc(
                bench, client.host, client_proc, f'client')

        bench.log(f'Clients started and running for {input.duration}.')

        # Wait for the client to finish and then terminate the server.
        client_proc.wait()
        server_proc.kill()
        if input.monitored:
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
