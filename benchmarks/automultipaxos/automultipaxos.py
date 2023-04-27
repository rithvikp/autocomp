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


class LeaderOptions(NamedTuple):
    flush_every_n: int
    p1a_node_0_timeout: int
    p1a_other_nodes_timeout: int
    i_am_leader_resend_timeout: int
    i_am_leader_check_timeout: int


class Input(NamedTuple):
    # System-wide parameters. ##################################################
    f: int
    num_client_procs: int
    num_warmup_clients_per_proc: int
    num_clients_per_proc: int
    num_leaders: int
    num_acceptors_per_partition: int
    num_acceptor_partitions: int
    num_replicas: int
    num_p2a_proxy_leaders_per_leader: int
    num_p2b_proxy_leaders_per_leader: int
    client_jvm_heap_size: str
    replica_jvm_heap_size: str

    # Benchmark parameters. ####################################################
    measurement_group_size: int
    start_lag: datetime.timedelta
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

    # Leader options. ##########################################################
    leader_options: LeaderOptions

    # Replica options. #########################################################
    replica_options: ReplicaOptions
    replica_log_level: str

    # Client options. ##########################################################
    client_options: ClientOptions
    client_log_level: str


class AutoMultiPaxosOutput(NamedTuple):
    read_output: benchmark.RecorderOutput
    write_output: benchmark.RecorderOutput


Output = AutoMultiPaxosOutput


# Networks #####################################################################
class AutoMultiPaxosNet:
    def __init__(self, inp: Input, endpoints: Dict[str, provision.EndpointProvider], run_index: int):
        self._input = inp
        self._endpoints = endpoints
        self._run_index = run_index

    def update(self, endpoints: Dict[str, provision.EndpointProvider]) -> None:
        self._endpoints = endpoints

    class Placement(NamedTuple):
        clients: List[host.Endpoint]
        leaders: List[host.Endpoint]
        acceptors: List[host.Endpoint]
        replicas: List[host.Endpoint]
        coordinators: List[host.Endpoint]
        p2a_proxy_leaders: List[host.Endpoint]
        p2b_proxy_leaders: List[host.Endpoint]

    def prom_placement(self) -> Placement:
        ports = itertools.count(40001+self._run_index, 100)

        def portify_one(e: host.PartialEndpoint) -> host.Endpoint:
            return host.Endpoint(e.host, next(ports) if self._input.monitored else -1)

        def portify(role: str, n: int) -> List[host.Endpoint]:
            return [portify_one(e) for e in self._endpoints[role].get_range(n, sender=-1)]

        return self.Placement(
            clients=portify('clients', self._input.num_client_procs),
            leaders=portify('leaders', self._input.num_leaders),
            acceptors=portify('acceptors', self._input.num_acceptors_per_partition*self._input.num_acceptor_partitions),
            replicas=portify('replicas', self._input.num_replicas),
            coordinators=portify('coordinators', self._input.num_acceptors_per_partition),
            p2a_proxy_leaders=portify('p2a_proxy_leaders', self._input.num_p2a_proxy_leaders_per_leader * self._input.num_leaders),
            p2b_proxy_leaders=portify('p2b_proxy_leaders', self._input.num_p2b_proxy_leaders_per_leader * self._input.num_leaders),
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
            leaders=portify('leaders', self._input.num_leaders),
            acceptors=portify('acceptors', self._input.num_acceptors_per_partition*self._input.num_acceptor_partitions),
            replicas=portify('replicas', self._input.num_replicas),
            # Num coordinators = num logical acceptors
            coordinators=portify('coordinators', self._input.num_acceptors_per_partition),
            p2a_proxy_leaders=portify('p2a_proxy_leaders', self._input.num_p2a_proxy_leaders_per_leader * self._input.num_leaders),
            p2b_proxy_leaders=portify('p2b_proxy_leaders', self._input.num_p2b_proxy_leaders_per_leader * self._input.num_leaders),
        )

    def config(self, index: int = 0) -> proto_util.Message:
        return {
            'f': self._input.f,
            'batcher_address': [],
            'read_batcher_address': [],
            'leader_address': [{
                'host': e.host.ip(),
                'port': e.port
            } for e in self.placement(index=index).leaders],
            'leader_election_address': [{
                'host': e.host.ip(),
                'port': e.port
            } for e in self.placement(index=index).leaders],
            # Placeholder
            'proxy_leader_address': [{
                'host': e.host.ip(),
                'port': e.port
            } for e in self.placement(index=index).leaders],
            'p2a_proxy_leader_address': [{
                'host': e.host.ip(),
                'port': e.port
            } for e in self.placement(index=index).p2a_proxy_leaders],
            'acceptor_address': [{
                'acceptor_address': [{
                    'host': e.host.ip(),
                    'port': e.port
                } for e in self.placement(index=index).acceptors[i*self._input.num_acceptors_per_partition:(i+1)*self._input.num_acceptors_per_partition]]
            } for i in range(self._input.num_acceptor_partitions)],
            'coordinator_address': [{
                'host': e.host.ip(),
                'port': e.port
            } for e in self.placement(index=index).coordinators],
            'p2b_proxy_leader_address': [{
                'host': e.host.ip(),
                'port': e.port
            } for e in self.placement(index=index).p2b_proxy_leaders],
            'replica_address': [{
                'host': e.host.ip(),
                'port': e.port
            } for e in self.placement(index=index).replicas],
            'proxy_replica_address': [],
            'flexible': False,
            'distribution_scheme': DistributionScheme.HASH,
        }


# Suite ########################################################################
class AutoMultiPaxosSuite(benchmark.Suite[Input, Output]):
    def __init__(self):
        self.run_index = 0
        super().__init__()

    def run_benchmark(self,
                      bench: benchmark.BenchmarkDirectory,
                      args: Dict[Any, Any],
                      input: Input) -> Output:
        assert input.f*2 + 1 == input.num_acceptors_per_partition
        net = AutoMultiPaxosNet(input, self.provisioner.hosts(input.f), self.run_index)

        prom_config_filename = bench.abspath('prom_config.txt')
        bench.write_string(prom_config_filename, proto_util.message_to_pbtext({k: str(v) for (k, v) in net._endpoints.items()}))
        bench.log("Finished assigning hosts. Now setting up hydroflow services.")

        self.run_index += 1

        # Pre-benchmark lag.
        time.sleep(input.start_lag.total_seconds())
        bench.log('Pre-benchmark lag ended.')

        # Launch acceptors.
        if self.service_type("acceptors") == "hydroflow":
            acceptor_procs: List[proc.Proc] = []
            for i in range(input.num_acceptors_per_partition):
                for (j, acceptor) in enumerate(net.prom_placement().acceptors[i*input.num_acceptor_partitions:(i+1)*input.num_acceptor_partitions]):
                    index = i*input.num_acceptor_partitions + j
                    acceptor_procs.append(self.provisioner.popen_hydroflow(bench, f'acceptors_{index}', input.f, [
                        '--service',
                        'acceptor',
                        '--acceptor.index',
                        str(i),
                        '--acceptor.coordinator-index',
                        str(i),
                        '--acceptor.partition-index',
                        str(index),
                        '--acceptor.num-p2b-proxy-leaders',
                        str(input.num_p2b_proxy_leaders_per_leader),
                        '--prometheus-host',
                        acceptor.host.ip(),
                        f'--prometheus-port={str(acceptor.port)}',
                    ]))
        else:
            raise ValueError("AutoMultiPaxos only supports hydroflow acceptors")

        # Launch leaders.
        leader_procs: List[proc.Proc] = []
        for (i, leader) in enumerate(net.prom_placement().leaders):
            leader_procs.append(self.provisioner.popen_hydroflow(bench, f'leaders_{i}', input.f, [
                '--service',
                'leader',
                '--leader.flush-every-n',
                str(input.leader_options.flush_every_n),
                # Vary timeouts to reduce contention
                '--leader.p1a-timeout',
                str(input.leader_options.p1a_node_0_timeout if i == 0 else input.leader_options.p1a_other_nodes_timeout),
                '--leader.i-am-leader-resend-timeout',
                str(input.leader_options.i_am_leader_resend_timeout),
                '--leader.i-am-leader-check-timeout',
                str(input.leader_options.i_am_leader_check_timeout),
                '--leader.index',
                str(i),
                '--leader.f',
                str(input.f),
                '--leader.num-acceptor-partitions',
                str(input.num_acceptor_partitions),
                '--leader.num-p2a-proxy-leaders-per-leader',
                str(input.num_p2a_proxy_leaders_per_leader),
                '--prometheus-host',
                leader.host.ip(),
                f'--prometheus-port={str(leader.port)}'
            ]))

        # Launch coordinators
        coordinator_procs: List[proc.Proc] = []
        for (i, coordinator) in enumerate(net.prom_placement().coordinators):
            coordinator_procs.append(self.provisioner.popen_hydroflow(bench, f'coordinators_{i}', input.f, [
                '--service',
                'coordinator',
                '--coordinator.index',
                str(i),
                '--coordinator.num-acceptor-partitions',
                str(input.num_acceptor_partitions),
                '--prometheus-host',
                coordinator.host.ip(),
                f'--prometheus-port={str(coordinator.port)}'
            ]))
        
        # Launch p2a proxy leaders
        p2a_proxy_leader_procs: List[proc.Proc] = []
        for (i, proxy_leader) in enumerate(net.prom_placement().p2a_proxy_leaders):
            p2a_proxy_leader_procs.append(self.provisioner.popen_hydroflow(bench, f'p2a_proxy_leaders_{i}', input.f, [
                '--service',
                'p2a-proxy-leader',
                '--p2a-proxy-leader.index',
                str(i),
                '--p2a-proxy-leader.num-acceptor-partitions',
                str(input.num_acceptor_partitions),
                '--prometheus-host',
                proxy_leader.host.ip(),
                f'--prometheus-port={str(proxy_leader.port)}'
            ]))

        # Launch p2b proxy leaders
        p2b_proxy_leader_procs: List[proc.Proc] = []
        for i in range(input.num_leaders):
            for (j, proxy_leader) in enumerate(net.prom_placement().p2b_proxy_leaders[i*input.num_p2b_proxy_leaders_per_leader:(i+1)*input.num_p2b_proxy_leaders_per_leader]):
                index = i*input.num_p2b_proxy_leaders_per_leader + j
                p2b_proxy_leader_procs.append(self.provisioner.popen_hydroflow(bench, f'p2b_proxy_leaders_{index}', input.f, [
                    '--service',
                    'p2b-proxy-leader',
                    '--p2b-proxy-leader.index',
                    str(index),
                    '--p2b-proxy-leader.leader-index',
                    str(i),
                    '--p2b-proxy-leader.f',
                    str(input.f),
                    '--p2b-proxy-leader.num-acceptor-partitions',
                    str(input.num_acceptor_partitions),
                    '--p2b-proxy-leader.num-acceptors',
                    str(input.num_acceptors_per_partition*input.num_acceptor_partitions),
                    '--prometheus-host',
                    proxy_leader.host.ip(),
                    f'--prometheus-port={str(proxy_leader.port)}'
                ]))


        bench.log("Reconfiguring the system for a new benchmark")
        endpoints, receive_endpoints = self.provisioner.rebuild(1, {
            "clients": ["leaders"],
            "leaders": ["p2a_proxy_leaders", "acceptors", "leaders"],
            "p2a_proxy_leaders": ["acceptors"],
            "p2b_proxy_leaders": ["leaders", "leaders", "replicas"],
            "coordinators": ["acceptors"],
            "acceptors": ["p2b_proxy_leaders", "coordinators", "leaders", "leaders"],
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
                    'automultipaxos_client': [
                        f'{e.host.ip()}:{e.port}'
                        for e in net.prom_placement().clients
                    ],
                    'automultipaxos_leader': [
                        f'{e.host.ip()}:{e.port}'
                        for e in net.prom_placement().leaders
                    ],
                    'automultipaxos_acceptor': [
                        f'{e.host.ip()}:{e.port}'
                        for e in net.prom_placement().acceptors
                    ],
                    'automultipaxos_replica': [
                        f'{e.host.ip()}:{e.port}'
                        for e in net.prom_placement().replicas
                    ],
                    'automultipaxos_coordinator': [
                        f'{e.host.ip()}:{e.port}'
                        for e in net.prom_placement().coordinators
                    ],
                    'automultipaxos_p2a_proxy_leader': [
                        f'{e.host.ip()}:{e.port}'
                        for e in net.prom_placement().p2a_proxy_leaders
                    ],
                    'automultipaxos_p2b_proxy_leader': [
                        f'{e.host.ip()}:{e.port}'
                        for e in net.prom_placement().p2b_proxy_leaders
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
                    # This is needed because Scala clients don't usually setup channels with every
                    # leader, but dedalus requires that.
                    '--receive_addrs',
                    ','.join([x.host.ip()+":"+str(x.port) for x in net.placement(index=i).leaders]),
                ])
            if input.profiled:
                p = perf_util.JavaPerfProc(bench, client.host, p, f'client_{i}')
            client_procs.append(p)
        bench.log(f'Clients started and running for {input.duration}.')

        # Wait for clients to finish and then terminate leaders and acceptors.
        for p in client_procs:
            p.wait()
        for p in (
            leader_procs + acceptor_procs + replica_procs + 
            coordinator_procs + p2a_proxy_leader_procs +
            p2b_proxy_leader_procs
        ):
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
        return AutoMultiPaxosOutput(read_output = read_output,
                                write_output = write_output)


def get_parser() -> argparse.ArgumentParser:
    return parser_util.get_benchmark_parser()
