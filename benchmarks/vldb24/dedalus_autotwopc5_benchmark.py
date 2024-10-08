from benchmarks.autotwopc.autotwopc import *

def main(args) -> None:
    class Suite(AutoTwoPCSuite):
        def __init__(self, args) -> None:
            self._args = args
            super().__init__()

        def args(self) -> Dict[Any, Any]:
            return vars(self._args)

        def cluster_spec(self) -> Dict[str, Dict[str, int]]:
            # Max across any benchmark
            return {
                '1': {
                    'leaders': 1,
                    'vote_requesters': 5,
                    'participant_voters': 3*5,
                    'committers': 5,
                    'participant_ackers': 3*5,
                    'enders': 5,
                    'clients': 5,
                },
            }
        
        def inputs(self) -> Collection[Input]:
            return [
                Input(
                    num_client_procs = num_client_procs,
                    num_clients_per_proc=num_clients_per_proc,
                    num_participants=num_participants,
                    num_vote_requesters=num_vote_requesters,
                    num_participant_voter_partitions=num_participant_voter_partitions,
                    num_committers=num_committers,
                    num_participant_acker_partitions=num_participant_acker_partitions,
                    num_enders=num_enders,
                    jvm_heap_size='100m',
                    duration=datetime.timedelta(seconds=60),
                    timeout=datetime.timedelta(seconds=65),
                    warmup_duration=datetime.timedelta(seconds=25),
                    warmup_timeout=datetime.timedelta(seconds=30),
                    warmup_sleep=datetime.timedelta(seconds=5),
                    # Need a large lag in order for Prometheus to initialize correctly
                    client_lag=datetime.timedelta(seconds=10),
                    log_level=self.args()['log_level'],
                    leader_flush_every_n=leader_flush_every_n,
                    profiled=self._args.profile,
                    monitored=self._args.monitor,
                    prometheus_scrape_interval=datetime.timedelta(
                        milliseconds=200),
                    workload =
                      read_write_workload.UniformReadWriteWorkload(
                        num_keys=1,
                        read_fraction=0.0,
                        write_size_mean=16,
                        write_size_std=0),
                )
                # for client_procs in [1, 10, 25, 40, 50, 60, 75, 100, 125, 150, 175]
                # for num_replicas in [3, 5]
                for (num_client_procs, num_clients_per_proc) in [
                    (1, 1),
                    (1, 50),
                    (1, 100),
                    (2, 100),
                    (3, 100),
                    (4, 100),
                    (5, 100),
                    # (6, 100),
                    # (7, 100),
                    # (8, 100),
                    # (9, 100),
                    # (10, 100),
                ]
                for num_participants in [3]
                # for num_vote_requesters in [3]
                # for num_participant_voter_partitions in [3]
                # for num_committers in [3]
                # for num_participant_acker_partitions in [3]
                # for num_enders in [3]
                for leader_flush_every_n in [1] #[15,1]
                for (num_vote_requesters, num_participant_voter_partitions, num_committers, num_participant_acker_partitions, num_enders) in [
                    # (1, 1, 1, 1, 1),
                    # (3, 3, 3, 3, 3),
                    (5, 5, 5, 5, 5),
                ]
            ] #*3

        def summary(self, input: Input, output: Output) -> str:
            return str({
                'value_size': input.workload,
                'num_client_procs': input.num_client_procs,
                'num_clients_per_proc': input.num_clients_per_proc,
                'num_participants': input.num_participants,
                'num_vote_requesters': input.num_vote_requesters,
                'num_participant_voter_partitions': input.num_participant_voter_partitions,
                'num_committers': input.num_committers,
                'num_participant_acker_partitions': input.num_participant_acker_partitions,
                'num_enders': input.num_enders,
                'leader_flush_every_n': input.leader_flush_every_n,
                'latency.median_ms': output.latency.median_ms,
                'start_throughput_1s.p90': output.start_throughput_1s.p90,
            })


    suite = Suite(args)
    with benchmark.SuiteDirectory(args.suite_directory,
                                  'autotwopc5_lt_dedalus') as dir:
        suite.run_suite(dir)


if __name__ == '__main__':
    main(get_parser().parse_args())
