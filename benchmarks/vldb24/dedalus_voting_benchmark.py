from benchmarks.voting.voting import *

def main(args) -> None:
    class Suite(VotingSuite):
        def __init__(self, args) -> None:
            self._args = args
            super().__init__()

        def args(self) -> Dict[Any, Any]:
            return vars(self._args)

        def cluster_spec(self) -> Dict[str, Dict[str, int]]:
            return {
                '1': {
                    'leaders': 1,
                    'replicas': 7, # Max across any benchmark
                    'clients': 10,
                },
            }
        
        def inputs(self) -> Collection[Input]:
            return [
                Input(
                    num_client_procs = num_client_procs,
                    num_clients_per_proc=num_clients_per_proc,
                    num_replicas=num_replicas,
                    jvm_heap_size='8g',
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
                    # (5, 100),
                    # (6, 100),
                    # (7, 100),
                    # (8, 100),
                    # (9, 100),
                    # (10, 100),
                ]
                for num_replicas in [7]
                for leader_flush_every_n in [1] #[15,1]
            ] *3

        def summary(self, input: Input, output: Output) -> str:
            return str({
                'value_size': input.workload,
                'num_client_procs': input.num_client_procs,
                'num_clients_per_proc': input.num_clients_per_proc,
                'num_replicas': input.num_replicas,
                'leader_flush_every_n': input.leader_flush_every_n,
                'latency.median_ms': output.latency.median_ms,
                'start_throughput_1s.p90': output.start_throughput_1s.p90,
            })


    suite = Suite(args)
    with benchmark.SuiteDirectory(args.suite_directory,
                                  'voting_lt_dedalus') as dir:
        suite.run_suite(dir)


if __name__ == '__main__':
    main(get_parser().parse_args())