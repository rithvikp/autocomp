from benchmarks.comppaxos.comppaxos import *

def main(args) -> None:
    class Suite(CompPaxosSuite):
        def __init__(self, args) -> None:
            self._args = args
            super().__init__()

        def args(self) -> Dict[Any, Any]:
            return vars(self._args)

        def cluster_spec(self) -> Dict[str, Dict[str, int]]:
            return {
                # Max across any benchmark
                '1': {
                    'leaders': 2,
                    'replicas': 4,
                    'clients': 7,
                    'acceptors': 4,
                    'proxy_leaders': 10,
                },
            }

        def inputs(self) -> Collection[Input]:
            return [
                Input(
                    f = 1,
                    num_client_procs = num_client_procs,
                    num_warmup_clients_per_proc = num_clients_per_proc,
                    num_clients_per_proc = num_clients_per_proc,
                    num_leaders = 2,
                    num_acceptor_rows = num_acceptor_rows,
                    num_acceptor_columns = num_acceptor_columns,
                    num_replicas = num_replicas,
                    num_proxy_leaders = num_proxy_leaders,
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
                    # state_machine = 'Noop',
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
                    leader_options = LeaderOptions(
                        flush_every_n = leader_flush_every_n,
                        p1a_node_0_timeout = 300,
                        p1a_other_nodes_timeout = 60000,
                        i_am_leader_resend_timeout = 2000,
                        i_am_leader_check_timeout = 5000,
                    ),
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
                    flexible = True,
                )
                
                for value_size in [16]
                for num_acceptor_rows in [2]
                for num_acceptor_columns in [2]
                for num_replicas in [4]
                for num_proxy_leaders in [10]
                for (num_client_procs, num_clients_per_proc, leader_flush_every_n) in [
                    (1, 1, 1),
                    (1, 50, 10),
                    (1, 100, 10),
                    (2, 100, 10),
                    (3, 100, 10),
                    (4, 100, 10),
                    (5, 100, 10),
                    (6, 100, 10),
                    (7, 100, 10),
                    # (8, 100, 10),
                    # (9, 100, 10),
                    # (10, 100, 10),
                ]
            ] * 3

        def summary(self, input: Input, output: Output) -> str:
            return str({
                'f': input.f,
                'value_size': input.workload,
                'num_client_procs': input.num_client_procs,
                'num_clients_per_proc': input.num_clients_per_proc,
                'num_acceptor_rows': input.num_acceptor_rows,
                'num_acceptor_columns': input.num_acceptor_columns,
                'num_proxy_leaders': input.num_proxy_leaders,
                'num_replicas': input.num_replicas,
                'leader_flush_every_n': input.leader_options.flush_every_n,
                'write.latency.median_ms': f'{output.write_output.latency.median_ms:.6}',
                'write.start_throughput_1s.p90': f'{output.write_output.start_throughput_1s.p90:.8}',
            })


    suite = Suite(args)
    with benchmark.SuiteDirectory(args.suite_directory,
                                  'comppaxos_lt_dedalus') as dir:
        suite.run_suite(dir)


if __name__ == '__main__':
    main(get_parser().parse_args())
