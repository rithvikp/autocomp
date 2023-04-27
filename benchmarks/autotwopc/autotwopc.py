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
    num_participants: int
    num_vote_requesters: int
    num_participant_voter_partitions: int
    num_committers: int
    num_participant_acker_partitions: int
    num_enders: int
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
class AutoTwoPCNet:
    def __init__(self, inp: Input, endpoints: Dict[str, provision.EndpointProvider], run_index: int):
        self._input = inp
        self._endpoints = endpoints
        self._run_index = run_index

    def update(self, endpoints: Dict[str, provision.EndpointProvider]) -> None:
        self._endpoints = endpoints

    class Placement(NamedTuple):
        clients: List[host.Endpoint]
        leader: host.Endpoint
        vote_requesters: List[host.Endpoint]
        participant_voters: List[host.Endpoint]
        committers: List[host.Endpoint]
        participant_ackers: List[host.Endpoint]
        enders: List[host.Endpoint]

    def prom_placement(self) -> Placement:
        ports = itertools.count(40001+self._run_index, 100)

        def portify_one(e: host.PartialEndpoint) -> host.Endpoint:
            return host.Endpoint(e.host, next(ports) if self._input.monitored else -1)

        def portify(role: str, n: int) -> List[host.Endpoint]:
            return [portify_one(e) for e in self._endpoints[role].get_range(n, sender=-1)]
        
        return self.Placement(
            clients=portify('clients', self._input.num_client_procs),
            leader=portify_one(self._endpoints['leaders'].get(0, sender=-1)),
            vote_requesters=portify('vote_requesters', self._input.num_vote_requesters),
            participant_voters=portify('participant_voters', self._input.num_participant_voter_partitions * self._input.num_participants),
            committers=portify('committers', self._input.num_committers),
            participant_ackers=portify('participant_ackers', self._input.num_participant_acker_partitions * self._input.num_participants),
            enders=portify('enders', self._input.num_enders),
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
            vote_requesters=portify('vote_requesters', self._input.num_vote_requesters),
            participant_voters=portify('participant_voters', self._input.num_participant_voter_partitions * self._input.num_participants),
            committers=portify('committers', self._input.num_committers),
            participant_ackers=portify('participant_ackers', self._input.num_participant_acker_partitions * self._input.num_participants),
            enders=portify('enders', self._input.num_enders),
        )

    def config(self, index: int = 0) -> proto_util.Message:
        return {
            'leader_address': {
                'host': self.placement(index=index).leader.host.ip(),
                'port': self.placement(index=index).leader.port,
            },
            'replica_address': [{
                'host': e.host.ip(),
                'port': e.port,
            } for e in self.placement(index = index).participant_ackers],
        }

# Suite ########################################################################
class AutoTwoPCSuite(benchmark.Suite[Input, Output]):
    def __init__(self):
        self.run_index = 0
        super().__init__()

    def run_benchmark(self, bench: benchmark.BenchmarkDirectory,
                      args: Dict[Any, Any], inp: Input) -> Output:
        net = AutoTwoPCNet(inp, self.provisioner.hosts(1), self.run_index)

        prom_config_filename = bench.abspath('prom_config.txt')
        bench.write_string(prom_config_filename, proto_util.message_to_pbtext({k: str(v) for (k, v) in net._endpoints.items()}))
        bench.log("Finished assigning hosts. Now setting up hydroflow services.")

        self.run_index += 1

        if self.service_type("leaders") == "hydroflow":
            leader_proc = self.provisioner.popen_hydroflow(bench, 'leaders', 1, [
                '--service',
                'leader',
                '--prometheus-host',
                net.prom_placement().leader.host.ip(),
                f'--prometheus-port={str(net.prom_placement().leader.port)}',
                '--flush-every-n',
                str(inp.leader_flush_every_n),
                '--leader.num-vote-requester-partitions',
                str(inp.num_vote_requesters),
            ])
        else:
            raise ValueError("AutoTwoPC only currently supports hydroflow leaders")

        if self.service_type("vote_requesters") == "hydroflow":
            vote_requester_procs: List[proc.Proc] = []
            for (i, vote_requester) in enumerate(net.prom_placement().vote_requesters):
                vote_requester_procs.append(self.provisioner.popen_hydroflow(bench, f'vote_requesters_{i}', 1, [
                    '--service',
                    'vote-requester',
                    '--prometheus-host',
                    vote_requester.host.ip(),
                    f'--prometheus-port={str(vote_requester.port)}',
                    '--vote-requester.num-participants',
                    str(inp.num_participants),
                    '--vote-requester.num-participant-voter-partitions',
                    str(inp.num_participant_voter_partitions),
                ]))
        else:
            raise ValueError("AutoTwoPC only currently supports hydroflow vote_requesters")
        
        if self.service_type("participant_voters") == "hydroflow":
            participant_voter_procs: List[proc.Proc] = []
            for (i, participant_voter) in enumerate(net.prom_placement().participant_voters):
                participant_voter_procs.append(self.provisioner.popen_hydroflow(bench, f'participant_voters_{i}', 1, [
                    '--service',
                    'participant-voter',
                    '--prometheus-host',
                    participant_voter.host.ip(),
                    f'--prometheus-port={str(participant_voter.port)}',
                    '--participant-voter.index',
                    str(i),
                    '--participant-voter.num-committers',
                    str(inp.num_committers),
                ]))
        else:
            raise ValueError("AutoTwoPC only currently supports hydroflow participant_voters")
        
        if self.service_type("committers") == "hydroflow":
            committer_procs: List[proc.Proc] = []
            for (i, committer) in enumerate(net.prom_placement().committers):
                committer_procs.append(self.provisioner.popen_hydroflow(bench, f'committers_{i}', 1, [
                    '--service',
                    'committer',
                    '--prometheus-host',
                    committer.host.ip(),
                    f'--prometheus-port={str(committer.port)}',
                    '--committer.num-participants',
                    str(inp.num_participants),
                    '--committer.num-participant-acker-partitions',
                    str(inp.num_participant_acker_partitions),
                ]))
        else:
            raise ValueError("AutoTwoPC only currently supports hydroflow committers")

        if self.service_type("participant_ackers") == "hydroflow":
            participant_acker_procs: List[proc.Proc] = []
            for (i, participant_acker) in enumerate(net.prom_placement().participant_ackers):
                participant_acker_procs.append(self.provisioner.popen_hydroflow(bench, f'participant_ackers_{i}', 1, [
                    '--service',
                    'participant-acker',
                    '--prometheus-host',
                    participant_acker.host.ip(),
                    f'--prometheus-port={str(participant_acker.port)}',
                    '--participant-acker.index',
                    str(i),
                    '--participant-acker.num-enders',
                    str(inp.num_enders),
                ]))
        else:
            raise ValueError("AutoTwoPC only currently supports hydroflow participant_ackers")
        
        if self.service_type("enders") == "hydroflow":
            ender_procs: List[proc.Proc] = []
            for (i, ender) in enumerate(net.prom_placement().enders):
                ender_procs.append(self.provisioner.popen_hydroflow(bench, f'enders_{i}', 1, [
                    '--service',
                    'ender',
                    '--prometheus-host',
                    ender.host.ip(),
                    f'--prometheus-port="{str(ender.port)}"',
                    '--ender.num-participants',
                    str(inp.num_participants),
                ]))
        else:
            raise ValueError("AutoTwoPC only currently supports hydroflow enders") 

        bench.log("Reconfiguring the system for a new benchmark")
        endpoints, receive_endpoints = self.provisioner.rebuild(1, {
            "clients": ["leaders"],
            "leaders": ["vote_requesters"],
            "vote_requesters": ["participant_voters"],
            "participant_voters": ["committers"],
            "committers": ["participant_ackers"],
            "participant_ackers": ["enders"],
            "enders": ["clients"],
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
                    'autotwopc_client': [
                        f'{e.host.ip()}:{e.port}'
                        for e in net.prom_placement().clients
                    ],
                    'autotwopc_leader': [
                        f'{net.prom_placement().leader.host.ip()}:' +
                        f'{net.prom_placement().leader.port}'
                    ],
                    'autotwopc_vote_requester': [
                        f'{e.host.ip()}:{e.port}'
                        for e in net.prom_placement().vote_requesters
                    ],
                    'autotwopc_participant_voter': [
                        f'{e.host.ip()}:{e.port}'
                        for e in net.prom_placement().participant_voters
                    ],
                    'autotwopc_committer': [
                        f'{e.host.ip()}:{e.port}'
                        for e in net.prom_placement().committers
                    ],
                    'autotwopc_participant_acker': [
                        f'{e.host.ip()}:{e.port}'
                        for e in net.prom_placement().participant_ackers
                    ],
                    'autotwopc_ender': [
                        f'{e.host.ip()}:{e.port}'
                        for e in net.prom_placement().enders
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
        for p in (vote_requester_procs+participant_voter_procs+committer_procs+participant_acker_procs+ender_procs):
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