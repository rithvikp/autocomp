from benchmarks.autovoting.autovoting import *

def main(args) -> None:
    class Suite(AutoVotingSuite):
        def __init__(self, args) -> None:
            self._args = args
            super().__init__()

        def args(self) -> Dict[Any, Any]:
            return vars(self._args)

        def cluster_spec(self) -> Dict[str, Dict[str, int]]:
            return {
                '1': {
                    'leaders': 1,
                    'replicas': 3*5, # Max across any benchmark
                    'collectors': 5, # Max across any benchmark
                    'broadcasters': 5, # Max across any benchmark
                    'clients': 8,
                },
            }

        def inputs(self) -> Collection[Input]:
            return [
                Input(
                num_client_procs = num_client_procs,
                num_clients_per_proc=num_clients_per_proc,
                num_replica_groups=replica_groups,
                num_replica_partitions=replica_partitions,
                num_broadcasters=broadcasters,
                num_collectors=collectors,
                jvm_heap_size='100m',
                warmup_duration=datetime.timedelta(seconds=25),
                warmup_timeout=datetime.timedelta(seconds=30),
                warmup_sleep=datetime.timedelta(seconds=5),
                duration=datetime.timedelta(seconds=60),
                timeout=datetime.timedelta(seconds=65),
                # Need a large lag in order for Prometheus to initialize correctly
                client_lag=datetime.timedelta(seconds=5),
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

            for replica_groups in [3]
            # for replica_partitions in [3]
            # for collectors in [3]
            # for broadcasters in [3]
            for leader_flush_every_n in [15]
            for (num_client_procs, num_clients_per_proc) in [
                (1, 1),
                (1, 50),
                (1, 100),
                (2, 100),
                (3, 100),
                (4, 100),
                (5, 100),
                (6, 100),
                (7, 100),
                (8, 100),
                # (9, 100),
                # (10, 100),
            ]
            for (replica_partitions, collectors, broadcasters) in [
                (5,5,5),
            ]

            ]#*3

        def summary(self, input: Input, output: Output) -> str:
            return str({
                'value_size': input.workload,
                'num_client_procs': input.num_client_procs,
                'num_clients_per_proc': input.num_clients_per_proc,
                'num_replica_groups': input.num_replica_groups,
                'num_replica_partitions': input.num_replica_partitions,
                'num_broadcasters': input.num_broadcasters,
                'num_collectors': input.num_collectors,
                'leader_flush_every_n': input.leader_flush_every_n,
                'latency.median_ms': output.latency.median_ms,
                'start_throughput_1s.p90': output.start_throughput_1s.p90,
            })


    suite = Suite(args)
    with benchmark.SuiteDirectory(args.suite_directory,
                                  'autovoting5_lt_dedalus') as dir:
        suite.run_suite(dir)


if __name__ == '__main__':
    main(get_parser().parse_args())