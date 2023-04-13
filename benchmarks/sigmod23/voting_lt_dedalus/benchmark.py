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
                    'clients': 1,
                },
            }

        def inputs(self) -> Collection[Input]:
            def gen_input(clients: int, replicas: int, leader_flush_every_n: int) -> Input:
                return Input(
                    num_clients_per_proc=clients,
                    num_replicas=replicas,
                    jvm_heap_size='100m',
                    duration=datetime.timedelta(seconds=60),
                    timeout=datetime.timedelta(seconds=120),
                    warmup_duration=datetime.timedelta(seconds=15),
                    warmup_timeout=datetime.timedelta(seconds=30),
                    warmup_sleep=datetime.timedelta(seconds=1),
                    # Need a large lag in order for Prometheus to initialize correctly
                    client_lag=datetime.timedelta(seconds=10),
                    log_level=self.args()['log_level'],
                    leader_flush_every_n=leader_flush_every_n,
                    profiled=self._args.profile,
                    monitored=self._args.monitor,
                    prometheus_scrape_interval=datetime.timedelta(
                        milliseconds=200),
                )


            return [
                gen_input(client_procs, num_replicas, leader_flush_every_n)

                # for client_procs in [1, 10, 25, 40, 50, 60, 75, 100, 125, 150, 175]
                # for num_replicas in [3, 5]

                for client_procs in [25, 50, 75, 125, 175, 250, 400]
                for num_replicas in [3,7]
                for leader_flush_every_n in [15,1]
            ]#*3

        def summary(self, input: Input, output: Output) -> str:
            return str({
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