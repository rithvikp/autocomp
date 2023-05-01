import json
from benchmarks.automicrobenchmarks.automicrobenchmarks import *


def main(args) -> None:
    class Suite(AutoMicrobenchmarksSuite):
        def __init__(self, args) -> None:
            self._args = args
            with open(self.args()['cluster_config'], 'r') as f:
                self.cluster_config = json.load(f)

            # Roles across all microbenchmarks
            self.cluster_config['services'] = {
                'clients': { 'type': 'scala' },
                'leaders': { 'type': 'hydroflow' },
                'replicas': { 'type': 'hydroflow' },
                'responders': { 'type': 'hydroflow' },
                'collectors': { 'type': 'hydroflow' },
            }

            super().__init__()

        def args(self) -> Dict[Any, Any]:
            return vars(self._args)

        def cluster_spec(self) -> Dict[str, Dict[str, int]]:
            # Max across all microbenchmarks
            return {
                '1': {
                    'leaders': 1,
                    'clients': 7,
                    'replicas': 9,
                    'responders': 1,
                    'collectors': 1,
                },
            }

        def inputs(self) -> Collection[Input]:
            system_inputs = dict(
                jvm_heap_size='8g',
                # duration=datetime.timedelta(seconds=60),
                # timeout=datetime.timedelta(seconds=65),
                # warmup_duration=datetime.timedelta(seconds=25),
                # warmup_timeout=datetime.timedelta(seconds=30),
                duration=datetime.timedelta(seconds=10),
                timeout=datetime.timedelta(seconds=15),
                warmup_duration=datetime.timedelta(seconds=10),
                warmup_timeout=datetime.timedelta(seconds=15),
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

            # Set these as necessary to run the correct set of microbenchmarks.
            run_decoupling_functional = False
            run_decoupling_monotonic = False
            run_decoupling_mutually_independent = False
            run_decoupling_state_machine = False
            run_decoupling_general = False
            run_partitioning_cohashing = True
            run_partitioning_dependencies = False
            run_partitioning_partial = False

            clients = [
                (1, 1),
                (1, 3),
                # (2, 3),
                # (3, 3),
                # (4, 3),
                # (5, 3),
                # (5, 1),
                # (6, 1),
                # (6, 1),
                # (7, 1),
                # (4, 15),
                # (5, 25),
                # (6, 25),
                # (7, 25),
                # (8, 25),
                # (9, 25),
                # (10, 100),
            ]

            if run_decoupling_functional:
                # Regular: (1, 3)
                # Auto: (2, 3)
                clients_special = [
                    (1, 1),
                    (1, 3),
                    (2, 3),
                    (3, 3),
                    (4, 3),
                    (5, 3),
                ]
                inputs.extend([
                    Input(
                        microbenchmark_type = MicrobenchmarkType.DECOUPLING_FUNCTIONAL,
                        num_client_procs = num_client_procs,
                        num_clients_per_proc=num_clients_per_proc,
                        **system_inputs,
                    )
                    for (num_client_procs, num_clients_per_proc) in clients_special
                ])
                inputs.extend([
                    Input(
                        microbenchmark_type = MicrobenchmarkType.AUTO_DECOUPLING_FUNCTIONAL,
                        num_client_procs = num_client_procs,
                        num_clients_per_proc=num_clients_per_proc,
                        **system_inputs,
                    )
                    for (num_client_procs, num_clients_per_proc) in clients_special
                ])

            if run_decoupling_monotonic:
                # Regular: (1, 3)
                # Auto: (3, 3)
                clients_special = [
                    (1, 1),
                    (1, 3),
                    (2, 3),
                    (3, 3),
                    (4, 3),
                    (5, 3),
                ]
                inputs.extend([
                    Input(
                        microbenchmark_type = MicrobenchmarkType.DECOUPLING_MONOTONIC,
                        num_client_procs = num_client_procs,
                        num_clients_per_proc=num_clients_per_proc,
                        decoupling_monotonic_options=DecouplingMonotonicOptions(
                            num_replicas=3,
                        ),
                        **system_inputs,
                    )
                    for (num_client_procs, num_clients_per_proc) in clients_special
                ])
                inputs.extend([
                    Input(
                        microbenchmark_type = MicrobenchmarkType.AUTO_DECOUPLING_MONOTONIC,
                        num_client_procs = num_client_procs,
                        num_clients_per_proc=num_clients_per_proc,
                        decoupling_monotonic_options=DecouplingMonotonicOptions(
                            num_replicas=3,
                        ),
                        **system_inputs,
                    )
                    for (num_client_procs, num_clients_per_proc) in clients_special
                ])

            if run_decoupling_mutually_independent:
                # Regular: (1, 15)
                # Auto:
                clients_special = [
                    (1, 1),
                    (1, 15),
                    (2, 15),
                    (3, 15),
                    (4, 15),
                    (5, 15),
                    (6, 15),
                    (7, 15),
                ]
                inputs.extend([
                    Input(
                        microbenchmark_type = MicrobenchmarkType.DECOUPLING_MUTUALLY_INDEPENDENT,
                        num_client_procs = num_client_procs,
                        num_clients_per_proc=num_clients_per_proc,
                        decoupling_mutually_independent_options=DecouplingMutuallyIndependentOptions(
                            num_replicas=3,
                        ),
                        **system_inputs,
                    )
                    for (num_client_procs, num_clients_per_proc) in clients_special
                ])
                inputs.extend([
                    Input(
                        microbenchmark_type = MicrobenchmarkType.AUTO_DECOUPLING_MUTUALLY_INDEPENDENT,
                        num_client_procs = num_client_procs,
                        num_clients_per_proc=num_clients_per_proc,
                        decoupling_mutually_independent_options=DecouplingMutuallyIndependentOptions(
                            num_replicas=3,
                        ),
                        **system_inputs,
                    )
                    for (num_client_procs, num_clients_per_proc) in clients_special
                ])

            if run_decoupling_state_machine:
            #     inputs.extend([
            #         Input(
            #             microbenchmark_type = MicrobenchmarkType.DECOUPLING_STATE_MACHINE,
            #             num_client_procs = num_client_procs,
            #             num_clients_per_proc=num_clients_per_proc,
            #             **system_inputs,
            #         )
            #         for (num_client_procs, num_clients_per_proc) in clients
            #     ])
                inputs.extend([
                    Input(
                        microbenchmark_type = MicrobenchmarkType.AUTO_DECOUPLING_STATE_MACHINE,
                        num_client_procs = num_client_procs,
                        num_clients_per_proc=num_clients_per_proc,
                        **system_inputs,
                    )
                    for (num_client_procs, num_clients_per_proc) in clients
                ])

            if run_decoupling_general:
                inputs.extend([
                    Input(
                        microbenchmark_type = MicrobenchmarkType.DECOUPLING_GENERAL,
                        num_client_procs = num_client_procs,
                        num_clients_per_proc=num_clients_per_proc,
                        **system_inputs,
                    )
                    for (num_client_procs, num_clients_per_proc) in clients
                ])

            if run_partitioning_cohashing:
                clients_special = [
                    # (1, 1),
                    # (1, 15),
                    (2, 15),
                    (3, 15),
                    (4, 15),
                    (5, 15),
                    # (6, 15),
                    # (7, 15),
                ]
                inputs.extend([
                    Input(
                        microbenchmark_type = MicrobenchmarkType.AUTO_PARTITIONING_COHASHING,
                        num_client_procs = num_client_procs,
                        num_clients_per_proc=num_clients_per_proc,
                        partitioning_cohashing_options=PartitioningCohashingOptions(
                            num_replicas=3,
                            num_partitions_per_replica=3,
                        ),
                        **system_inputs,
                    )
                    for (num_client_procs, num_clients_per_proc) in clients_special
                ])
                inputs.extend([
                    Input(
                        microbenchmark_type = MicrobenchmarkType.PARTITIONING_COHASHING,
                        num_client_procs = num_client_procs,
                        num_clients_per_proc=num_clients_per_proc,
                        partitioning_cohashing_options=PartitioningCohashingOptions(
                            num_replicas=3,
                        ),
                        **system_inputs,
                    )
                    for (num_client_procs, num_clients_per_proc) in clients_special
                ])

            if run_partitioning_dependencies:
                clients_special = [
                    (1, 1),
                    # (1, 15),
                    (2, 15),
                    (3, 15),
                    (4, 15),
                    (5, 15),
                    (6, 15),
                    (7, 15),
                ]
                inputs.extend([
                    Input(
                        microbenchmark_type = MicrobenchmarkType.AUTO_PARTITIONING_DEPENDENCIES,
                        num_client_procs = num_client_procs,
                        num_clients_per_proc=num_clients_per_proc,
                        partitioning_dependencies_options=PartitioningDependenciesOptions(
                            num_replicas=3,
                            num_partitions_per_replica=3,
                        ),
                        **system_inputs,
                    )
                    for (num_client_procs, num_clients_per_proc) in clients_special
                ])
                inputs.extend([
                    Input(
                        microbenchmark_type = MicrobenchmarkType.PARTITIONING_DEPENDENCIES,
                        num_client_procs = num_client_procs,
                        num_clients_per_proc=num_clients_per_proc,
                        partitioning_dependencies_options=PartitioningDependenciesOptions(
                            num_replicas=3,
                        ),
                        **system_inputs,
                    )
                    for (num_client_procs, num_clients_per_proc) in clients_special
                ])

            if run_partitioning_partial:
                inputs.extend([
                    Input(
                        microbenchmark_type = MicrobenchmarkType.PARTITIONING_PARTIAL,
                        num_client_procs = num_client_procs,
                        num_clients_per_proc=num_clients_per_proc,
                        partitioning_partial_options=PartitioningPartialOptions(
                            num_replicas=3,
                        ),
                        **system_inputs,
                    )
                    for (num_client_procs, num_clients_per_proc) in clients
                ])

            return inputs #*3

                

        def summary(self, input: Input, output: Output) -> str:
            return str({
                'type': str(input.microbenchmark_type),
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