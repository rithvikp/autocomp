from benchmarks.autopbft.autopbft_critical_path import *

def main(args) -> None:
    class Suite(AutoPBFTCriticalPathSuite):
        def __init__(self, args) -> None:
            self._args = args
            super().__init__()

        def args(self) -> Dict[Any, Any]:
            return vars(self._args)

        def cluster_spec(self) -> Dict[str, Dict[str, int]]:
            return {
                '1': {
                    'replicas': 4, # Max across any benchmark
                    'clients': 1,
                    'leaders': 4,
                    'prepreparers': 4*2,
                    'preparers': 4*2,
                    'committers': 4*2,
                },
            }

        def inputs(self) -> Collection[Input]:
            return [
                Input(
                    f = 1,
                    num_client_procs = num_client_procs,
                    num_warmup_clients_per_proc = num_clients_per_proc,
                    num_clients_per_proc = num_clients_per_proc,
                    num_pbft_replicas = 4,
                    num_prepreparers_per_pbft_replica = num_prepreparers_per_pbft_replica,
                    num_preparers_per_pbft_replica = num_preparers_per_pbft_replica,
                    num_committers_per_pbft_replica = num_committers_per_pbft_replica,
                    num_acceptors = 3,
                    num_replicas = num_replicas,
                    client_jvm_heap_size = '8g',
                    replica_jvm_heap_size = '12g',
                    measurement_group_size = 10,
                    start_lag = datetime.timedelta(seconds=0),
                    warmup_duration = datetime.timedelta(seconds=25),
                    warmup_timeout = datetime.timedelta(seconds=30),
                    warmup_sleep = datetime.timedelta(seconds=5),
                    duration = datetime.timedelta(seconds=60),
                    timeout = datetime.timedelta(seconds=65),
                    client_lag = datetime.timedelta(seconds=5),
                    state_machine = 'KeyValueStore',
                    predetermined_read_fraction = -1,
                    workload_label = 'write_only',
                    workload =
                      read_write_workload.UniformReadWriteWorkload(
                        num_keys=1,
                        read_fraction=0.0,
                        write_size_mean=value_size,
                        write_size_std=0),
                    read_workload =
                      read_write_workload.UniformReadWriteWorkload(
                        num_keys=1,
                        read_fraction=1.0,
                        write_size_mean=value_size,
                        write_size_std=0),
                    write_workload =
                      read_write_workload.UniformReadWriteWorkload(
                        num_keys=1,
                        read_fraction=0.0,
                        write_size_mean=value_size,
                        write_size_std=0),
                    read_consistency = 'linearizable',
                    profiled = args.profile,
                    monitored = args.monitor,
                    prometheus_scrape_interval =
                        datetime.timedelta(milliseconds=200),
                    replica_options = ReplicaOptions(
                        log_grow_size = 5000,
                        unsafe_dont_use_client_table = False,
                        send_chosen_watermark_every_n_entries = 100,
                        recover_log_entry_min_period = \
                            datetime.timedelta(seconds=2),
                        recover_log_entry_max_period = \
                            datetime.timedelta(seconds=5),
                        unsafe_dont_recover = False,
                    ),
                    replica_log_level = args.log_level,
                    client_options = ClientOptions(
                        resend_client_request_period =
                            datetime.timedelta(seconds=1),
                        resend_max_slot_requests_period =
                            datetime.timedelta(seconds=1),
                        resend_read_request_period =
                            datetime.timedelta(seconds=1),
                        resend_sequential_read_request_period =
                            datetime.timedelta(seconds=1),
                        resend_eventual_read_request_period =
                            datetime.timedelta(seconds=1),
                        unsafe_read_at_first_slot = False,
                        unsafe_read_at_i = False,
                        flush_writes_every_n = 1,
                        flush_reads_every_n = 1,
                    ),
                    client_log_level = args.log_level,
                    bft = True,
                )

                for value_size in [16]
                for num_prepreparers_per_pbft_replica in [2]
                for num_preparers_per_pbft_replica in [2]
                for num_committers_per_pbft_replica in [2]
                for num_replicas in [4]
                for (num_client_procs, num_clients_per_proc) in [
                    (1, 1),
                    (1, 50),
                    (1, 100),
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
            ] *3

        def summary(self, input: Input, output: Output) -> str:
            return str({
                'f': input.f,
                'value_size': input.workload,
                'num_client_procs': input.num_client_procs,
                'num_clients_per_proc': input.num_clients_per_proc,
                'num_leaders': input.num_pbft_replicas,
                'num_prepreparers_per_pbft_replica': input.num_prepreparers_per_pbft_replica,
                'num_preparers_per_pbft_replica': input.num_preparers_per_pbft_replica,
                'num_committers_per_pbft_replica': input.num_committers_per_pbft_replica,
                'num_replicas': input.num_replicas,
                'write.latency.median_ms': f'{output.write_output.latency.median_ms:.6}',
                'write.start_throughput_1s.p90': f'{output.write_output.start_throughput_1s.p90:.8}',
                'bft': input.bft,
            })


    suite = Suite(args)
    with benchmark.SuiteDirectory(args.suite_directory,
                                  'autopbft_critical_path_lt_dedalus') as dir:
        suite.run_suite(dir)


if __name__ == '__main__':
    main(get_parser().parse_args())