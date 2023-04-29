import json
from benchmarks.automicrobenchmarks.microbenchmarks import *


def main(args) -> None:
    class Suite(MicrobenchmarksSuite):
        def __init__(self, args) -> None:
            self._args = args
            with open(self.args()['cluster_config'], 'r') as f:
                self.cluster_config = json.load(f)

            # Roles across all microbenchmarks
            self.cluster_config['services'] = {
                'clients': { 'type': 'scala'},
                'leaders': { 'type': 'hydroflow', 'rust_example': 'decoupling_functional' },
            }

            super().__init__()

        def args(self) -> Dict[Any, Any]:
            return vars(self._args)

        def cluster_spec(self) -> Dict[str, Dict[str, int]]:
            # Max across all microbenchmarks
            return {
                '1': {
                    'leaders': 1,
                    'clients': 2,
                    # 'replicas': 3, 
                },
            }

        def inputs(self) -> Collection[Input]:
            system_inputs = dict(
                jvm_heap_size='100m',
                duration=datetime.timedelta(seconds=60),
                timeout=datetime.timedelta(seconds=65),
                warmup_duration=datetime.timedelta(seconds=25),
                warmup_timeout=datetime.timedelta(seconds=30),
                warmup_sleep=datetime.timedelta(seconds=5),
                # Need a large lag in order for Prometheus to initialize correctly
                client_lag=datetime.timedelta(seconds=10),
                log_level=self.args()['log_level'],
                profiled=self._args.profile,
                monitored=self._args.monitor,
                prometheus_scrape_interval=datetime.timedelta(
                    milliseconds=200),
            )
            inputs = []

            inputs.extend([
                Input(
                    microbenchmark_type = MicrobenchmarkType.DECOUPLING_FUNCTIONAL,
                    num_client_procs = num_client_procs,
                    num_clients_per_proc=num_clients_per_proc,
                    **system_inputs,
                )
                for (num_client_procs, num_clients_per_proc) in [
                    # (1, 1),
                    # (1, 50),
                    # (1, 100),
                    (2, 100),
                    # (3, 100),
                    # (4, 100),
                    # (5, 100),
                    # (6, 100),
                    # (7, 100),
                    # (8, 100),
                    # (9, 100),
                    # (10, 100),
                ]
            ])

            # inputs.extend([
            #     Input(
            #         microbenchmark_type = MicrobenchmarkType.DECOUPLING_FUNCTIONAL,
            #         num_client_procs = num_client_procs,
            #         num_clients_per_proc=num_clients_per_proc,
            #         **system_inputs,
            #     )
            #     for (num_client_procs, num_clients_per_proc) in [
            #         # (1, 1),
            #         (1, 50),
            #         (1, 100),
            #         (2, 100),
            #         # (3, 100),
            #         # (4, 100),
            #         # (5, 100),
            #         # (6, 100),
            #         # (7, 100),
            #         # (8, 100),
            #         # (9, 100),
            #         # (10, 100),
            #     ]
            # ])

            # Add new inputs here

            return inputs

                

        def summary(self, input: Input, output: Output) -> str:
            return str({
                'type': input.microbenchmark_type,
                'num_client_procs': input.num_client_procs,
                'num_clients_per_proc': input.num_clients_per_proc,
                'latency.median_ms': output.latency.median_ms,
                'start_throughput_1s.p90': output.start_throughput_1s.p90,
            })

    suite = Suite(args)
    with benchmark.SuiteDirectory(args.suite_directory,
                                  'microbenchmarks_lt_dedalus') as dir:
        suite.run_suite(dir)


if __name__ == '__main__':
    main(get_parser().parse_args())