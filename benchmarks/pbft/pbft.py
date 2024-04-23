# Copied from dedalus_multipaxos.py, since it uses the same clients & replicas
from .. import benchmark
from .. import cluster
from .. import host
from .. import parser_util
from .. import pd_util
from .. import perf_util
from .. import proc
from .. import prometheus
from .. import proto_util
from .. import read_write_workload
from .. import util
from .. import provision
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

class DistributionScheme(enum.Enum):
    HASH = 'HASH'
    COLOCATED = 'COLOCATED'

# Input/Output #################################################################
class ClientOptions(NamedTuple):
    resend_client_request_period: datetime.timedelta = \
        datetime.timedelta(seconds=1)
    resend_max_slot_requests_period: datetime.timedelta = \
        datetime.timedelta(seconds=1)
    resend_read_request_period: datetime.timedelta = \
        datetime.timedelta(seconds=1)
    resend_sequential_read_request_period: datetime.timedelta = \
        datetime.timedelta(seconds=1)
    resend_eventual_read_request_period: datetime.timedelta = \
        datetime.timedelta(seconds=1)
    unsafe_read_at_first_slot: bool = False
    unsafe_read_at_i: bool = False
    flush_writes_every_n: int = 1
    flush_reads_every_n: int = 1


class ReplicaOptions(NamedTuple):
    log_grow_size: int = 1000
    unsafe_dont_use_client_table: bool = False
    send_chosen_watermark_every_n_entries: int = 100
    recover_log_entry_min_period: datetime.timedelta = \
        datetime.timedelta(seconds=10)
    recover_log_entry_max_period: datetime.timedelta = \
        datetime.timedelta(seconds=20)
    unsafe_dont_recover: bool = False



class Input(NamedTuple):
    # System-wide parameters. ##################################################
    f: int
    k: int
    num_client_procs: int
    num_warmup_clients_per_proc: int
    num_clients_per_proc: int
    num_pbft_replicas: int
    num_acceptors: int
    num_replicas: int
    client_jvm_heap_size: str
    replica_jvm_heap_size: str
    bft: bool
    # full_bft: bool

    # Benchmark parameters. ####################################################
    measurement_group_size: int
    warmup_duration: datetime.timedelta
    warmup_timeout: datetime.timedelta
    warmup_sleep: datetime.timedelta
    duration: datetime.timedelta
    timeout: datetime.timedelta
    client_lag: datetime.timedelta
    state_machine: str
    predetermined_read_fraction: int
    workload_label: str
    workload: read_write_workload.ReadWriteWorkload
    read_workload: read_write_workload.ReadWriteWorkload
    write_workload: read_write_workload.ReadWriteWorkload
    read_consistency: str # "linearizable", "sequential", or "eventual"
    profiled: bool
    monitored: bool
    prometheus_scrape_interval: datetime.timedelta

    # Replica options. #########################################################
    replica_options: ReplicaOptions
    replica_log_level: str

    # Client options. ##########################################################
    client_options: ClientOptions
    client_log_level: str


class DedalusPBFTOutput(NamedTuple):
    read_output: benchmark.RecorderOutput
    write_output: benchmark.RecorderOutput


Output = DedalusPBFTOutput


# Networks #####################################################################
class DedalusPBFTNet:
    def __init__(self, inp: Input, endpoints: Dict[str, provision.EndpointProvider]):
        self._input = inp
        self._endpoints = endpoints

    def update(self, endpoints: Dict[str, provision.EndpointProvider]) -> None:
        self._endpoints = endpoints

    class Placement(NamedTuple):
        clients: List[host.Endpoint]
        pbft_replicas: List[host.Endpoint]
        replicas: List[host.Endpoint]

    def prom_placement(self) -> Placement:
        ports = itertools.count(40001, 100)

        def portify_one(e: host.PartialEndpoint) -> host.Endpoint:
            return host.Endpoint(e.host, next(ports) if self._input.monitored else -1)

        def portify(role: str, n: int) -> List[host.Endpoint]:
            return [portify_one(e) for e in self._endpoints[role].get_range(n, sender=-1)]

        return self.Placement(
            clients=portify('clients', self._input.num_client_procs),
            pbft_replicas=portify('pbft_replicas', self._input.num_pbft_replicas),
            replicas=portify('replicas', self._input.num_replicas),
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
            pbft_replicas=portify('pbft_replicas', self._input.num_pbft_replicas),
            replicas=portify('replicas', self._input.num_replicas),
        )

    def config(self, index: int = 0) -> proto_util.Message:
        pbft_replicas_only_3 = self.placement(index=index).pbft_replicas[0:3]
        return {
            'f': self._input.f,
            'batcher_address': [],
            'read_batcher_address': [],
            'leader_address': [{
                'host': e.host.ip(),
                'port': e.port
            } for e in self.placement(index=index).pbft_replicas],
            'leader_election_address': [{
                'host': e.host.ip(),
                'port': e.port
            } for e in self.placement(index=index).pbft_replicas],
            'proxy_leader_address': [{
                'host': e.host.ip(),
                'port': e.port
            } for e in self.placement(index=index).pbft_replicas],
            'acceptor_address': [{
                'acceptor_address': [{
                    'host': e.host.ip(),
                    'port': e.port
                } for e in pbft_replicas_only_3]
            }],
            'replica_address': [{
                'host': e.host.ip(),
                'port': e.port
            } for e in self.placement(index=index).replicas],
            'proxy_replica_address': [],
            'flexible': False,
            'distribution_scheme': DistributionScheme.HASH,
        }


# Suite ########################################################################
class DedalusPBFTSuite(benchmark.Suite[Input, Output]):
    def run_benchmark(self,
                      bench: benchmark.BenchmarkDirectory,
                      args: Dict[Any, Any],
                      input: Input) -> Output:
        net = DedalusPBFTNet(input, self.provisioner.hosts(input.f))

        # Launch pbft_replicas.
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
                    '--pbft_replica.k',
                    str(input.k),
                    '--pbft_replica.num_pbft_replicas',
                    str(input.num_pbft_replicas),
                    '--prometheus-host',
                    pbft_replica.host.ip(),
                    '--prometheus-port',
                    str(pbft_replica.port),
                ]))
        else:
            raise ValueError("DedalusPBFTSuite only supports hydroflow pbft_replicas")

        bench.log("Reconfiguring the system for a new benchmark")
        endpoints, receive_endpoints = self.provisioner.rebuild(1, {
            "clients": ["pbft_replicas"],
            "pbft_replicas": ["pbft_replicas", "pbft_replicas", "pbft_replicas", "replicas"],
            "replicas": ["clients"],
        },
        {
            "clients": input.num_client_procs,
            "replicas": input.num_replicas,
        })
        net.update(endpoints)
        bench.log("Reconfiguration completed")

        # Write config file.
        config = net.config()
        config_filename = bench.abspath('config.pbtxt')
        bench.write_string(config_filename,
                           proto_util.message_to_pbtext(config))
        bench.log('Config file config.pbtxt written.')

        client_config_filenames = []
        for i in range(input.num_client_procs):
            filename = bench.abspath(f"client_config_{i}.pbtxt")
            bench.write_string(filename,
                           proto_util.message_to_pbtext(net.config(index=i)))
            client_config_filenames.append(filename)
        bench.log('Client-specific config files written.')

        def java(heap_size: str) -> List[str]:
            cmd = ['java', f'-Xms{heap_size}', f'-Xmx{heap_size}']
            if input.monitored:
                cmd += [
                    '-verbose:gc',
                    '-XX:-PrintGC',
                    '-XX:+PrintHeapAtGC',
                    '-XX:+PrintGCDetails',
                    '-XX:+PrintGCTimeStamps',
                    '-XX:+PrintGCDateStamps',
                ]
            return cmd

        # Launch replicas.
        replica_procs: List[proc.Proc] = []
        for (i, replica) in enumerate(net.placement().replicas):
            p = bench.popen(
                host=replica.host,
                label=f'replicas_{i}',
                cmd=java(input.replica_jvm_heap_size) + [
                    '-cp',
                    os.path.abspath(args['jar']),
                    'frankenpaxos.multipaxos.ReplicaMain',
                    '--index',
                    str(i),
                    '--config',
                    config_filename,
                    '--log_level',
                    input.replica_log_level,
                    '--state_machine',
                    input.state_machine,
                    '--prometheus_host',
                    net.prom_placement().replicas[i].host.ip(),
                    '--prometheus_port',
                    str(net.prom_placement().replicas[i].port),
                    '--options.logGrowSize',
                    str(input.replica_options.log_grow_size),
                    '--options.unsafeDontUseClientTable',
                    str(input.replica_options.unsafe_dont_use_client_table),
                    '--options.sendChosenWatermarkEveryNEntries',
                    str(input.replica_options.
                        send_chosen_watermark_every_n_entries),
                    '--options.recoverLogEntryMinPeriod',
                    '{}s'.format(input.replica_options.
                                 recover_log_entry_min_period.total_seconds()),
                    '--options.recoverLogEntryMaxPeriod',
                    '{}s'.format(input.replica_options.
                                 recover_log_entry_max_period.total_seconds()),
                    '--options.unsafeDontRecover',
                    str(input.replica_options.unsafe_dont_recover),
                    '--options.connectToLeader',
                    str(False),
                    '--options.bft',
                    f'{input.bft}',
                    # '--options.fullBft',
                    # f'{input.full_bft}',
                    '--receive_addrs',
                    ','.join([str(x) for x in receive_endpoints[i]]),
                ],
            )
            if input.profiled:
                p = perf_util.JavaPerfProc(bench, replica.host, p,
                                           f'replica_{i}')
            replica_procs.append(p)
        bench.log('Replicas started.')

        # Launch Prometheus.
        if input.monitored:
            prometheus_config = prometheus.prometheus_config(
                int(input.prometheus_scrape_interval.total_seconds() * 1000), {
                    'multipaxos_client': [
                        f'{e.host.ip()}:{e.port}'
                        for e in net.prom_placement().clients
                    ],
                    'pbft_replica': [
                        f'{e.host.ip()}:{e.port}'
                        for e in net.prom_placement().pbft_replicas
                    ],
                    'multipaxos_replica': [
                        f'{e.host.ip()}:{e.port}'
                        for e in net.prom_placement().replicas
                    ],
                })
            bench.write_string('prometheus.yml', yaml.dump(prometheus_config))
            prometheus_server = bench.popen(
                host=net.placement().clients[0].host,
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
        time.sleep(input.client_lag.total_seconds())
        bench.log('Client lag ended.')

        # Launch clients.
        workload_filename = bench.abspath('workload.pbtxt')
        bench.write_string(
            workload_filename,
            proto_util.message_to_pbtext(input.workload.to_proto()))
        read_workload_filename = bench.abspath('read_workload.pbtxt')
        bench.write_string(
            read_workload_filename,
            proto_util.message_to_pbtext(input.read_workload.to_proto()))
        write_workload_filename = bench.abspath('write_workload.pbtxt')
        bench.write_string(
            write_workload_filename,
            proto_util.message_to_pbtext(input.write_workload.to_proto()))

        client_procs: List[proc.Proc] = []
        for i in range(input.num_client_procs):
            client = net.placement(index=i).clients[i]
            p = bench.popen(
                host=client.host,
                label=f'clients_{i}',
                # TODO(mwhittaker): For now, we don't run clients with large
                # heaps and verbose garbage collection because they are all
                # colocated on one machine.
                cmd=java(input.client_jvm_heap_size) + [
                    '-cp',
                    os.path.abspath(args['jar']),
                    'frankenpaxos.multipaxos.ClientMain',
                    '--host',
                    client.host.ip(),
                    '--port',
                    str(client.port),
                    '--config',
                    client_config_filenames[i],
                    '--log_level',
                    input.client_log_level,
                    '--prometheus_host',
                    net.prom_placement().clients[i].host.ip(),
                    '--prometheus_port',
                    str(net.prom_placement().clients[i].port),
                    '--measurement_group_size',
                    f'{input.measurement_group_size}',
                    '--warmup_duration',
                    f'{input.warmup_duration.total_seconds()}s',
                    '--warmup_timeout',
                    f'{input.warmup_timeout.total_seconds()}s',
                    '--warmup_sleep',
                    f'{input.warmup_sleep.total_seconds()}s',
                    '--num_warmup_clients',
                    f'{input.num_warmup_clients_per_proc}',
                    '--duration',
                    f'{input.duration.total_seconds()}s',
                    '--timeout',
                    f'{input.timeout.total_seconds()}s',
                    '--num_clients',
                    f'{input.num_clients_per_proc}',
                    '--output_file_prefix',
                    bench.abspath(f'client_{i}'),
                    '--read_consistency',
                    f'{input.read_consistency}',
                    '--predetermined_read_fraction',
                    f'{input.predetermined_read_fraction}',
                    '--workload',
                    f'{workload_filename}',
                    '--read_workload',
                    f'{read_workload_filename}',
                    '--write_workload',
                    f'{write_workload_filename}',
                    '--options.resendClientRequestPeriod',
                    '{}s'.format(input.client_options.
                                 resend_client_request_period.total_seconds()),
                    '--options.resendMaxSlotRequestsPeriod',
                    '{}s'.format(input.client_options.
                                 resend_max_slot_requests_period.total_seconds()),
                    '--options.resendReadRequestPeriod',
                    '{}s'.format(input.client_options.
                                 resend_read_request_period.total_seconds()),
                    '--options.resendSequentialReadRequestPeriod',
                    '{}s'.format(input.client_options.
                                 resend_sequential_read_request_period.
                                 total_seconds()),
                    '--options.resendEventualReadRequestPeriod',
                    '{}s'.format(input.client_options.
                                 resend_eventual_read_request_period.
                                 total_seconds()),
                    '--options.unsafeReadAtFirstSlot',
                    f'{input.client_options.unsafe_read_at_first_slot}',
                    '--options.unsafeReadAtI',
                    f'{input.client_options.unsafe_read_at_i}',
                    '--options.flushWritesEveryN',
                    f'{input.client_options.flush_writes_every_n}',
                    '--options.flushReadsEveryN',
                    f'{input.client_options.flush_reads_every_n}',
                    '--options.bft',
                    f'{input.bft}',
                    # '--options.fullBft',
                    # f'{input.full_bft}',
                    # This is needed because Scala clients don't usually setup channels with every
                    # pbft_replica, but dedalus requires that.
                    '--receive_addrs',
                    ','.join([x.host.ip()+":"+str(x.port) for x in net.placement(index=i).pbft_replicas]),
                ])
            if input.profiled:
                p = perf_util.JavaPerfProc(bench, client.host, p, f'client_{i}')
            client_procs.append(p)
        bench.log(f'Clients started and running for {input.duration}.')

        # Wait for clients to finish and then terminate pbft_replicas.
        for p in client_procs:
            p.wait()
        for p in (pbft_replica_procs + replica_procs):
            p.kill()
        if input.monitored:
            prometheus_server.kill()
        bench.log('Clients finished and processes terminated.')

        # Client i writes results to `client_i_data.csv`.
        client_csvs = [
            bench.abspath(f'client_{i}_data.csv')
            for i in range(input.num_client_procs)
        ]

        dummy_latency = benchmark.LatencyOutput(
            mean_ms = -1.0,
            median_ms = -1.0,
            min_ms = -1.0,
            max_ms = -1.0,
            p90_ms = -1.0,
            p95_ms = -1.0,
            p99_ms = -1.0,
        )
        dummy_throughput = benchmark.ThroughputOutput(
            mean = -1.0,
            median = -1.0,
            min = -1.0,
            max = -1.0,
            p90 = -1.0,
            p95 = -1.0,
            p99 = -1.0,
        )
        dummy_output = benchmark.RecorderOutput(
            latency = dummy_latency,
            start_throughput_1s = dummy_throughput,
        )

        labeled_data = benchmark.parse_labeled_recorder_data(
            bench,
            client_csvs,
            drop_prefix=datetime.timedelta(seconds=0),
            save_data=False)
        read_output = (labeled_data['read']
                       if 'read' in labeled_data
                       else dummy_output)
        write_output = (labeled_data['write']
                        if 'write' in labeled_data
                        else dummy_output)
        return DedalusPBFTOutput(read_output = read_output,
                                write_output = write_output)


def get_parser() -> argparse.ArgumentParser:
    return parser_util.get_benchmark_parser()
