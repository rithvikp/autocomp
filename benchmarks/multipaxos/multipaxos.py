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
class DistributionScheme(enum.Enum):
    HASH = 'HASH'
    COLOCATED = 'COLOCATED'


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


class BatcherOptions(NamedTuple):
    batch_size: int = 1


class ReadBatcherOptions(NamedTuple):
    read_batching_scheme: str = "size,1,10s"
    unsafe_read_at_first_slot: bool = False
    unsafe_read_at_i: bool = False


class ElectionOptions(NamedTuple):
    ping_period: datetime.timedelta = datetime.timedelta(seconds=1)
    no_ping_timeout_min: datetime.timedelta = datetime.timedelta(seconds=5)
    no_ping_timeout_max: datetime.timedelta = datetime.timedelta(seconds=10)


class LeaderOptions(NamedTuple):
    resend_phase1as_period: datetime.timedelta = datetime.timedelta(seconds=1)
    flush_phase2as_every_n: int = 1
    noop_flush_period: datetime.timedelta = datetime.timedelta(seconds=0)
    election_options: ElectionOptions = ElectionOptions()


class ProxyLeaderOptions(NamedTuple):
    flush_phase2as_every_n: int = 1


class AcceptorOptions(NamedTuple):
    pass


class ReplicaOptions(NamedTuple):
    log_grow_size: int = 1000
    unsafe_dont_use_client_table: bool = False
    send_chosen_watermark_every_n_entries: int = 100
    recover_log_entry_min_period: datetime.timedelta = \
        datetime.timedelta(seconds=10)
    recover_log_entry_max_period: datetime.timedelta = \
        datetime.timedelta(seconds=20)
    unsafe_dont_recover: bool = False


class ProxyReplicaOptions(NamedTuple):
    flush_every_n: int = 1
    batch_flush: bool = False


class Input(NamedTuple):
    # System-wide parameters. ##################################################
    f: int
    num_client_procs: int
    num_warmup_clients_per_proc: int
    num_clients_per_proc: int
    num_batchers: int
    num_read_batchers: int
    num_leaders: int
    num_proxy_leaders: int
    num_acceptor_groups: int
    num_acceptors_per_group: int
    num_replicas: int
    num_proxy_replicas: int
    flexible: bool
    distribution_scheme: DistributionScheme
    client_jvm_heap_size: str
    batcher_jvm_heap_size: str
    read_batcher_jvm_heap_size: str
    leader_jvm_heap_size: str
    proxy_leader_jvm_heap_size: str
    acceptor_jvm_heap_size: str
    replica_jvm_heap_size: str
    proxy_replica_jvm_heap_size: str

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

    # Batcher options. #########################################################
    batcher_options: BatcherOptions
    batcher_log_level: str

    # ReadBatcher options. #####################################################
    read_batcher_options: ReadBatcherOptions
    read_batcher_log_level: str

    # Leader options. ##########################################################
    leader_options: LeaderOptions
    leader_log_level: str

    # ProxyLeader options. #####################################################
    proxy_leader_options: ProxyLeaderOptions
    proxy_leader_log_level: str

    # Acceptor options. ########################################################
    acceptor_options: AcceptorOptions
    acceptor_log_level: str

    # Replica options. #########################################################
    replica_options: ReplicaOptions
    replica_log_level: str

    # ProxyReplica options. ####################################################
    proxy_replica_options: ProxyReplicaOptions
    proxy_replica_log_level: str

    # Client options. ##########################################################
    client_options: ClientOptions
    client_log_level: str


class MultiPaxosOutput(NamedTuple):
    read_output: benchmark.RecorderOutput
    write_output: benchmark.RecorderOutput


Output = MultiPaxosOutput


# Networks #####################################################################
class MultiPaxosNet:
    def __init__(self, cluster: cluster.Cluster, input: Input) -> None:
        self._cluster = cluster.f(input.f)
        self._input = input

    class Placement(NamedTuple):
        clients: List[host.Endpoint]
        batchers: List[host.Endpoint]
        read_batchers: List[host.Endpoint]
        leaders: List[host.Endpoint]
        leader_elections: List[host.Endpoint]
        proxy_leaders: List[host.Endpoint]
        acceptors: List[List[host.Endpoint]]
        replicas: List[host.Endpoint]
        proxy_replicas: List[host.Endpoint]

    def placement(self) -> Placement:
        ports = itertools.count(10000, 100)

        def portify(hosts: List[host.Host]) -> List[host.Endpoint]:
            return [host.Endpoint(h, next(ports)) for h in hosts]

        def cycle_take_n(n: int, hosts: List[host.Host]) -> List[host.Host]:
            return list(itertools.islice(itertools.cycle(hosts), n))

        def chunks(xs, n):
            # https://stackoverflow.com/a/312464/3187068
            result = []
            for i in range(0, len(xs), n):
                result.append(xs[i:i + n])
            return result

        n = 2 * self._input.f + 1
        return self.Placement(
            clients=portify(
                cycle_take_n(self._input.num_client_procs,
                             self._cluster['clients'])),
            batchers=portify(
                cycle_take_n(self._input.num_batchers,
                             self._cluster['batchers'])),
            read_batchers=portify(
                cycle_take_n(self._input.num_read_batchers,
                             self._cluster['read_batchers'])),
            leaders=portify(
                cycle_take_n(self._input.num_leaders,
                             self._cluster['leaders'])),
            leader_elections=portify(
                cycle_take_n(self._input.num_leaders,
                             self._cluster['leaders'])),
            proxy_leaders=portify(
                cycle_take_n(self._input.num_proxy_leaders,
                             self._cluster['proxy_leaders'])),
            acceptors=chunks(
                portify(cycle_take_n(self._input.num_acceptor_groups *
                                     self._input.num_acceptors_per_group,
                                     self._cluster['acceptors'])),
                self._input.num_acceptors_per_group),
            replicas=portify(
                cycle_take_n(self._input.num_replicas,
                             self._cluster['replicas'])),
            proxy_replicas=portify(
                cycle_take_n(self._input.num_proxy_replicas,
                             self._cluster['proxy_replicas'])),
        )

    def config(self) -> proto_util.Message:

        return {
            'f': self._input.f,
            'batcher_address': [{
                'host': e.host.ip(),
                'port': e.port
            } for e in self.placement().batchers],
            'read_batcher_address': [{
                'host': e.host.ip(),
                'port': e.port
            } for e in self.placement().read_batchers],
            'leader_address': [{
                'host': e.host.ip(),
                'port': e.port
            } for e in self.placement().leaders],
            'leader_election_address': [{
                'host': e.host.ip(),
                'port': e.port
            } for e in self.placement().leader_elections],
            'proxy_leader_address': [{
                'host': e.host.ip(),
                'port': e.port
            } for e in self.placement().proxy_leaders],
            'acceptor_address': [{
                'acceptor_address': [{
                    'host': e.host.ip(),
                    'port': e.port
                } for e in group]
            } for group in self.placement().acceptors],
            'replica_address': [{
                'host': e.host.ip(),
                'port': e.port
            } for e in self.placement().replicas],
            'proxy_replica_address': [{
                'host': e.host.ip(),
                'port': e.port
            } for e in self.placement().proxy_replicas],
            'flexible': self._input.flexible,
            'distribution_scheme': self._input.distribution_scheme,
        }


# Suite ########################################################################
class MultiPaxosSuite(benchmark.Suite[Input, Output]):
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

    def run_benchmark(self,
                      bench: benchmark.BenchmarkDirectory,
                      args: Dict[Any, Any],
                      input: Input) -> Output:
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

        # Write config file.
        net = MultiPaxosNet(self._cluster, input)
        config = net.config()
        config_filename = bench.abspath('config.pbtxt')
        bench.write_string(config_filename,
                           proto_util.message_to_pbtext(config))
        bench.log('Config file config.pbtxt written.')

        # Launch acceptors.
        acceptor_procs: List[proc.Proc] = []
        for (group_index, group) in enumerate(net.placement().acceptors):
            for (i, acceptor) in enumerate(group):
                p = bench.popen(
                    host=acceptor.host,
                    label=f'acceptor_{group_index}_{i}',
                    cmd=java(input.acceptor_jvm_heap_size) + [
                        '-cp',
                        os.path.abspath(args['jar']),
                        'frankenpaxos.multipaxos.AcceptorMain',
                        '--group_index',
                        str(group_index),
                        '--index',
                        str(i),
                        '--config',
                        config_filename,
                        '--log_level',
                        input.acceptor_log_level,
                        '--prometheus_host',
                        acceptor.host.ip(),
                        '--prometheus_port',
                        str(acceptor.port + 1) if input.monitored else '-1',
                    ],
                )
                if input.profiled:
                    p = perf_util.JavaPerfProc(bench, acceptor.host, p,
                                               f'acceptor_{group_index}_{i}')
                acceptor_procs.append(p)
        bench.log('Acceptors started.')

        # Launch batchers.
        batcher_procs: List[proc.Proc] = []
        for (i, batcher) in enumerate(net.placement().batchers):
            p = bench.popen(
                host=batcher.host,
                label=f'batcher_{i}',
                cmd=java(input.batcher_jvm_heap_size) + [
                    '-cp',
                    os.path.abspath(args['jar']),
                    'frankenpaxos.multipaxos.BatcherMain',
                    '--index',
                    str(i),
                    '--config',
                    config_filename,
                    '--log_level',
                    input.batcher_log_level,
                    '--prometheus_host',
                    batcher.host.ip(),
                    '--prometheus_port',
                    str(batcher.port + 1) if input.monitored else '-1',
                    '--options.batchSize',
                    str(input.batcher_options.batch_size),
                ],
            )
            if input.profiled:
                p = perf_util.JavaPerfProc(bench, batcher.host, p,
                                           f'batcher_{i}')
            batcher_procs.append(p)
        bench.log('Batchers started.')

        # Launch read_batchers.
        read_batcher_procs: List[proc.Proc] = []
        for (i, read_batcher) in enumerate(net.placement().read_batchers):
            p = bench.popen(
                host=read_batcher.host,
                label=f'read_batcher_{i}',
                cmd=java(input.read_batcher_jvm_heap_size) + [
                    '-cp',
                    os.path.abspath(args['jar']),
                    'frankenpaxos.multipaxos.ReadBatcherMain',
                    '--index',
                    str(i),
                    '--config',
                    config_filename,
                    '--log_level',
                    input.read_batcher_log_level,
                    '--prometheus_host',
                    read_batcher.host.ip(),
                    '--prometheus_port',
                    str(read_batcher.port + 1) if input.monitored else '-1',
                    '--options.readBatchingScheme',
                    f'{input.read_batcher_options.read_batching_scheme}',
                    '--options.unsafeReadAtFirstSlot',
                    f'{input.read_batcher_options.unsafe_read_at_first_slot}',
                    '--options.unsafeReadAtI',
                    f'{input.read_batcher_options.unsafe_read_at_i}',
                ],
            )
            if input.profiled:
                p = perf_util.JavaPerfProc(bench, read_batcher.host, p,
                                           f'read_batcher_{i}')
            read_batcher_procs.append(p)
        bench.log('ReadBatchers started.')

        # Launch proxy_leaders.
        proxy_leader_procs: List[proc.Proc] = []
        for (i, proxy_leader) in enumerate(net.placement().proxy_leaders):
            p = bench.popen(
                host=proxy_leader.host,
                label=f'proxy_leader_{i}',
                cmd=java(input.proxy_leader_jvm_heap_size) + [
                    '-cp',
                    os.path.abspath(args['jar']),
                    'frankenpaxos.multipaxos.ProxyLeaderMain',
                    '--index',
                    str(i),
                    '--config',
                    config_filename,
                    '--log_level',
                    input.proxy_leader_log_level,
                    '--prometheus_host',
                    proxy_leader.host.ip(),
                    '--prometheus_port',
                    str(proxy_leader.port + 1) if input.monitored else '-1',
                    '--options.flushPhase2asEveryN',
                    str(input.proxy_leader_options.flush_phase2as_every_n),
                ],
            )
            if input.profiled:
                p = perf_util.JavaPerfProc(bench, proxy_leader.host, p,
                                           f'proxy_leader_{i}')
            proxy_leader_procs.append(p)
        bench.log('ProxyLeaders started.')

        # Launch replicas.
        replica_procs: List[proc.Proc] = []
        for (i, replica) in enumerate(net.placement().replicas):
            p = bench.popen(
                host=replica.host,
                label=f'replica_{i}',
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
                    replica.host.ip(),
                    '--prometheus_port',
                    str(replica.port + 1) if input.monitored else '-1',
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
                ],
            )
            if input.profiled:
                p = perf_util.JavaPerfProc(bench, replica.host, p,
                                           f'replica_{i}')
            replica_procs.append(p)
        bench.log('Replicas started.')

        # Launch proxy_replicas.
        proxy_replica_procs: List[proc.Proc] = []
        for (i, proxy_replica) in enumerate(net.placement().proxy_replicas):
            p = bench.popen(
                host=proxy_replica.host,
                label=f'proxy_replica_{i}',
                cmd=java(input.proxy_replica_jvm_heap_size) + [
                    '-cp',
                    os.path.abspath(args['jar']),
                    'frankenpaxos.multipaxos.ProxyReplicaMain',
                    '--index',
                    str(i),
                    '--config',
                    config_filename,
                    '--log_level',
                    input.proxy_replica_log_level,
                    '--prometheus_host',
                    proxy_replica.host.ip(),
                    '--prometheus_port',
                    str(proxy_replica.port + 1) if input.monitored else '-1',
                    '--options.flushEveryN',
                    str(input.proxy_replica_options.flush_every_n),
                    '--options.batchFlush',
                    str(input.proxy_replica_options.batch_flush),
                ],
            )
            if input.profiled:
                p = perf_util.JavaPerfProc(bench, proxy_replica.host, p,
                                           f'proxy_replica_{i}')
            proxy_replica_procs.append(p)
        bench.log('ProxyReplicas started.')

        # Launch leaders.
        leader_procs: List[proc.Proc] = []
        for (i, leader) in enumerate(net.placement().leaders):
            p = bench.popen(
                host=leader.host,
                label=f'leader_{i}',
                cmd=java(input.leader_jvm_heap_size) + [
                    '-cp',
                    os.path.abspath(args['jar']),
                    'frankenpaxos.multipaxos.LeaderMain',
                    '--index',
                    str(i),
                    '--config',
                    config_filename,
                    '--log_level',
                    input.leader_log_level,
                    '--prometheus_host',
                    leader.host.ip(),
                    '--prometheus_port',
                    str(leader.port + 1) if input.monitored else '-1',
                    '--options.resendPhase1asPeriod',
                    '{}s'.format(input.leader_options.resend_phase1as_period.
                                 total_seconds()),
                    '--options.flushPhase2asEveryN',
                    str(input.leader_options.flush_phase2as_every_n),
                    '--options.noopFlushPeriod',
                    '{}s'.format(input.leader_options.noop_flush_period.
                                 total_seconds()),
                    '--options.election.pingPeriod',
                    '{}s'.format(input.leader_options.election_options.
                                 ping_period.total_seconds()),
                    '--options.election.noPingTimeoutMin',
                    '{}s'.format(input.leader_options.election_options.
                                 no_ping_timeout_min.total_seconds()),
                    '--options.election.noPingTimeoutMax',
                    '{}s'.format(input.leader_options.election_options.
                                 no_ping_timeout_max.total_seconds()),
                ],
            )
            if input.profiled:
                p = perf_util.JavaPerfProc(bench, leader.host, p, f'leader_{i}')
            leader_procs.append(p)
        bench.log('Leaders started.')

        # Launch Prometheus.
        if input.monitored:
            prometheus_config = prometheus.prometheus_config(
                int(input.prometheus_scrape_interval.total_seconds() * 1000), {
                    'multipaxos_client': [
                        f'{e.host.ip()}:{e.port+1}'
                        for e in net.placement().clients
                    ],
                    'multipaxos_batcher': [
                        f'{e.host.ip()}:{e.port+1}'
                        for e in net.placement().batchers
                    ],
                    'multipaxos_read_batcher': [
                        f'{e.host.ip()}:{e.port+1}'
                        for e in net.placement().read_batchers
                    ],
                    'multipaxos_leader': [
                        f'{e.host.ip()}:{e.port+1}'
                        for e in net.placement().leaders
                    ],
                    'multipaxos_proxy_leader': [
                        f'{e.host.ip()}:{e.port+1}'
                        for e in net.placement().proxy_leaders
                    ],
                    'multipaxos_acceptor': [
                        f'{e.host.ip()}:{e.port+1}'
                        for group in net.placement().acceptors
                        for e in group
                    ],
                    'multipaxos_replica': [
                        f'{e.host.ip()}:{e.port+1}'
                        for e in net.placement().replicas
                    ],
                    'multipaxos_proxy_replica': [
                        f'{e.host.ip()}:{e.port+1}'
                        for e in net.placement().proxy_replicas
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
        for (i, client) in enumerate(net.placement().clients):
            p = bench.popen(
                host=client.host,
                label=f'client_{i}',
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
                    config_filename,
                    '--log_level',
                    input.client_log_level,
                    '--prometheus_host',
                    client.host.ip(),
                    '--prometheus_port',
                    str(client.port + 1) if input.monitored else '-1',
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
                ])
            if input.profiled:
                p = perf_util.JavaPerfProc(bench, client.host, p, f'client_{i}')
            client_procs.append(p)
        bench.log(f'Clients started and running for {input.duration}.')

        # Wait for clients to finish and then terminate leaders and acceptors.
        for p in client_procs:
            p.wait()
        for p in (batcher_procs + read_batcher_procs + leader_procs +
                  proxy_leader_procs + acceptor_procs + replica_procs +
                  proxy_replica_procs):
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
        return MultiPaxosOutput(read_output = read_output,
                                write_output = write_output)


def get_parser() -> argparse.ArgumentParser:
    return parser_util.get_benchmark_parser()
