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
                    'replicas': 10, # Max across any benchmark
                    'collectors': 3, # Max across any benchmark
                    'broadcasters': 3, # Max across any benchmark
                    'clients': 1,
                },
            }

        def inputs(self) -> Collection[Input]:
            def gen_input(clients: int, replica_groups: int, replica_partitions: int, collectors: int, broadcasters: int) -> Input:
                return Input(
                    num_clients_per_proc=clients,
                    num_replica_groups=replica_groups,
                    num_replica_partitions=replica_partitions,
                    num_broadcasters=broadcasters,
                    num_collectors=collectors,
                    jvm_heap_size='100m',
                    duration=datetime.timedelta(seconds=60),
                    timeout=datetime.timedelta(seconds=120),
                    warmup_duration=datetime.timedelta(seconds=15),
                    warmup_timeout=datetime.timedelta(seconds=30),
                    warmup_sleep=datetime.timedelta(seconds=1),
                    # Need a large lag in order for Prometheus to initialize correctly
                    client_lag=datetime.timedelta(seconds=10),
                    log_level=self.args()['log_level'],
                    profiled=self._args.profile,
                    monitored=self._args.monitor,
                    prometheus_scrape_interval=datetime.timedelta(
                        milliseconds=200),
                )


            return [
                gen_input(client_procs, replica_groups, replica_partitions, collectors, broadcasters)

                for client_procs in [100, 300, 500]
                for replica_groups in [3,5,10]
                for replica_partitions in [1]
                for collectors in [3]
                for broadcasters in [3]
            ]#*3

        def summary(self, input: Input, output: Output) -> str:
            return str({
                'num_clients_per_proc': input.num_clients_per_proc,
                'num_replica_groups': input.num_replica_groups,
                'num_replica_partitions': input.num_replica_partitions,
                'num_broadcasters': input.num_broadcasters,
                'num_collectors': input.num_collectors,
                'latency.median_ms': output.latency.median_ms,
                'start_throughput_1s.p90': output.start_throughput_1s.p90,
            })


    suite = Suite(args)
    with benchmark.SuiteDirectory(args.suite_directory,
                                  'autovoting_lt_dedalus') as dir:
        suite.run_suite(dir)


if __name__ == '__main__':
    main(get_parser().parse_args())