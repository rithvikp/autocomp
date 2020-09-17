from ...multipaxos.multipaxos import *


def main(args) -> None:
    class Suite(MultiPaxosSuite):
        def args(self) -> Dict[Any, Any]:
            return vars(args)

        def inputs(self) -> Collection[Input]:
            return [
                Input(
                    f = 1,
                    num_client_procs = num_client_procs,
                    num_warmup_clients_per_proc = num_clients_per_proc,
                    num_clients_per_proc = num_clients_per_proc,
                    num_batchers = num_batchers,
                    num_read_batchers = 0,
                    num_leaders = 2,
                    num_proxy_leaders = num_proxy_leaders,
                    num_acceptor_groups = num_acceptor_groups,
                    num_acceptors_per_group = num_acceptors_per_group,
                    num_replicas = num_replicas,
                    num_proxy_replicas = num_proxy_replicas,
                    flexible = True,
                    distribution_scheme = DistributionScheme.HASH,
                    client_jvm_heap_size = '8g',
                    batcher_jvm_heap_size = '12g',
                    read_batcher_jvm_heap_size = '12g',
                    leader_jvm_heap_size = '12g',
                    proxy_leader_jvm_heap_size = '12g',
                    acceptor_jvm_heap_size = '12g',
                    replica_jvm_heap_size = '12g',
                    proxy_replica_jvm_heap_size = '12g',
                    measurement_group_size = 10,
                    warmup_duration = datetime.timedelta(seconds=10),
                    warmup_timeout = datetime.timedelta(seconds=15),
                    warmup_sleep = datetime.timedelta(seconds=5),
                    duration = datetime.timedelta(seconds=15),
                    timeout = datetime.timedelta(seconds=20),
                    client_lag = datetime.timedelta(seconds=5),
                    state_machine = 'KeyValueStore',
                    predetermined_read_fraction = -1,
                    workload_label = 'write_only',
                    workload =
                      read_write_workload.UniformReadWriteWorkload(
                        num_keys=1,
                        read_fraction=0.0,
                        write_size_mean=16,
                        write_size_std=0),
                    read_workload =
                      read_write_workload.UniformReadWriteWorkload(
                        num_keys=1,
                        read_fraction=1.0,
                        write_size_mean=16,
                        write_size_std=0),
                    write_workload =
                      read_write_workload.UniformReadWriteWorkload(
                        num_keys=1,
                        read_fraction=0.0,
                        write_size_mean=16,
                        write_size_std=0),
                    read_consistency = 'linearizable',
                    profiled = args.profile,
                    monitored = args.monitor,
                    prometheus_scrape_interval =
                        datetime.timedelta(milliseconds=200),
                    batcher_options = BatcherOptions(
                        batch_size = batch_size,
                    ),
                    batcher_log_level = args.log_level,
                    read_batcher_options = ReadBatcherOptions(
                        read_batching_scheme = "size,1,10s",
                        unsafe_read_at_first_slot = False,
                        unsafe_read_at_i = False,
                    ),
                    read_batcher_log_level = args.log_level,
                    leader_options = LeaderOptions(
                        resend_phase1as_period = datetime.timedelta(seconds=1),
                        flush_phase2as_every_n = leader_flush_every_n,
                        election_options = ElectionOptions(
                            ping_period = datetime.timedelta(seconds=60),
                            no_ping_timeout_min = \
                                datetime.timedelta(seconds=120),
                            no_ping_timeout_max = \
                                datetime.timedelta(seconds=240),
                        ),
                    ),
                    leader_log_level = args.log_level,
                    proxy_leader_options = ProxyLeaderOptions(
                        flush_phase2as_every_n = proxy_leader_flush_every_n,
                    ),
                    proxy_leader_log_level = args.log_level,
                    acceptor_options = AcceptorOptions(),
                    acceptor_log_level = args.log_level,
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
                    proxy_replica_options = ProxyReplicaOptions(
                        flush_every_n = proxy_replica_flush_every_n,
                        # batch_flush = True,
                    ),
                    proxy_replica_log_level = args.log_level,
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
                )

                for (num_client_procs, num_clients_per_proc) in [(7, 100)]
                for num_batchers in [0]
                for batch_size in [0]
                for num_proxy_replicas in [0]
                for proxy_leader_flush_every_n in [0]
                for proxy_replica_flush_every_n in [0]

                for num_acceptor_groups in [2]
                for num_acceptors_per_group in [5]
                for leader_flush_every_n in [10]
                for num_replicas in [6, 7, 8, 9, 10]
                for num_proxy_leaders in [8, 9, 10, 11, 12, 13, 14, 15]


                ## for (
                ##     num_client_procs,           # 1
                ##     num_clients_per_proc,       # 2
                ##     num_batchers,               # 3
                ##     num_proxy_leaders,          # 4
                ##     num_acceptor_groups,        # 5
                ##     num_acceptors_per_group,    # 6
                ##     num_replicas,               # 7
                ##     num_proxy_replicas,         # 8
                ##     batch_size,                 # 9
                ##     leader_flush_every_n,       # 10
                ##     proxy_leader_flush_every_n, # 11
                ##     proxy_replica_flush_every_n # 12
                ## ) in [
                ##     # 1    2   3   4   5   6   7   8   9  10  11  12
                ##     ( 7, 100,  0,  5,  2,  2,  2,  0,  0, 20,  1,  1),
                ##     ( 7, 100,  0,  5,  2,  2,  2,  2,  0, 20,  1,  1),
##
                ##     # more acceptors
                ##     # ( 7, 100,  0,  5,  2,  3,  2,  0,  0, 20,  1,  1),
                ##     # ( 7, 100,  0,  5,  2,  4,  2,  0,  0, 20,  1,  1),
                ##     # ( 7, 100,  0,  6,  2,  3,  2,  0,  0, 20,  1,  1),
                ##     # ( 7, 100,  0,  6,  2,  4,  2,  0,  0, 20,  1,  1),
                ##     # more replicas
                ##     # ( 7, 100,  0,  5,  2,  3,  3,  0,  0, 20,  1,  1),
                ##     # ( 7, 100,  0,  5,  2,  4,  3,  0,  0, 20,  1,  1),
                ##     # ( 7, 100,  0,  6,  2,  3,  3,  0,  0, 20,  1,  1),
                ##     # ( 7, 100,  0,  6,  2,  4,  3,  0,  0, 20,  1,  1),
##
##
                ##     # ( 1,  10, 0, 30, 2, 5, 5, 10, 0, 1, 1, 1),
                ##     # ( 5,  10, 0, 30, 2, 5, 5, 10, 0, 5, 5, 1),
                ##     # ( 5,  20, 0, 30, 2, 5, 5, 10, 0, 10, 10, 1),
                ##     # ( 6,  50, 0, 30, 2, 5, 5, 10, 0, 10, 10, 1),
                ##     # ( 6, 100, 0, 30, 2, 5, 5, 10, 0, 10, 10, 1),
                ##     # (10, 100, 0, 30, 2, 5, 5, 10, 0, 10, 10, 1),
                ##     # (20, 100, 0, 30, 2, 5, 5, 10, 0, 10, 10, 1),
##
                ##     # ( 1,   1, 0, 30, 3, 5, 10, 0, 1, 1, 1),
                ##     # ( 1,  10, 0, 30, 3, 5, 10, 0, 1, 1, 1),
                ##     # ( 5,  10, 0, 30, 3, 5, 10, 0, 5, 5, 1),
                ##     # ( 5,  20, 0, 30, 3, 5, 10, 0, 10, 10, 1),
                ##     # ( 6,  50, 0, 30, 3, 5, 10, 0, 10, 10, 1),
                ##     # ( 6, 100, 0, 30, 3, 5, 10, 0, 10, 10, 1),
                ##     # (10, 100, 0, 30, 3, 5, 10, 0, 10, 10, 1),
                ##     # (20, 100, 0, 30, 3, 5, 10, 0, 10, 10, 1),
##
                ##     # ( 1,   1, 2, 23, 3, 5, 10, 1, 1, 1, 1),
                ##     # ( 1,  10, 2, 23, 3, 5, 10, 1, 1, 1, 1),
                ##     # ( 5,  10, 2, 23, 3, 5, 10, 10, 1, 1, 1),
                ##     # ( 5,  20, 2, 23, 3, 5, 10, 10, 1, 1, 1),
                ##     # ( 6,  50, 2, 23, 3, 5, 10, 10, 1, 1, 1),
                ##     # ( 6, 100, 2, 23, 3, 5, 10, 10, 1, 1, 1),
                ##     # (10, 100, 4, 23, 3, 5, 10, 10, 1, 1, 1),
                ##     # (20, 100, 4, 23, 3, 5, 10, 20, 1, 1, 1),
                ##     # (20, 200, 8, 23, 3, 5, 10, 40, 1, 1, 1),
                ## ]
            ] * 3

        def summary(self, input: Input, output: Output) -> str:
            return str({
                'f':
                    input.f,
                'num_client_procs':
                    input.num_client_procs,
                'num_clients_per_proc':
                    input.num_clients_per_proc,
                'num_batchers':
                    input.num_batchers,
                'num_proxy_leaders':
                    input.num_proxy_leaders,
                'num_acceptor_groups':
                    input.num_acceptor_groups,
                'num_replicas':
                    input.num_replicas,
                'num_proxy_replicas':
                    input.num_proxy_replicas,
                'batch_size':
                    input.batcher_options.batch_size,
                'leader_flush_every_n':
                    input.leader_options.flush_phase2as_every_n,
                'proxy_leader_flush_every_n':
                    input.proxy_leader_options.flush_phase2as_every_n,
                'proxy_replica_flush_every_n':
                    input.proxy_replica_options.flush_every_n,
                'write.latency.median_ms': \
                    f'{output.write_output.latency.median_ms:.6}',
                'write.start_throughput_1s.p90': \
                    f'{output.write_output.start_throughput_1s.p90:.8}',
                'read.latency.median_ms': \
                    f'{output.read_output.latency.median_ms:.6}',
                'read.start_throughput_1s.p90': \
                    f'{output.read_output.start_throughput_1s.p90:.8}',
            })

    suite = Suite()
    with benchmark.SuiteDirectory(args.suite_directory,
                                  'compartmentalized_lt') as dir:
        suite.run_suite(dir)


if __name__ == '__main__':
    main(get_parser().parse_args())
