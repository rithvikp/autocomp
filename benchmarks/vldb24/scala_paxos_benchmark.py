from benchmarks.multipaxos.multipaxos import *

# hpython -m benchmarks.vldb24.scala_paxos_benchmark -j /mnt/nfs/tmp/frankenpaxos-assembly-0.1.0-SNAPSHOT.jar -m -s /mnt/nfs/tmp/ -l info --cluster_config ../clusters/multipaxos/scala_hydro_config.json

# python -m benchmarks.vldb24.scala_paxos_benchmark -j /mnt/nfs/tmp/frankenpaxos-assembly-0.1.0-SNAPSHOT.jar -m -s /mnt/nfs/tmp/ -l info --cluster_config ../clusters/multipaxos/scala_config.json

def main(args) -> None:
    class Suite(MultiPaxosSuite):
        def __init__(self) -> None:
            self._args = args
            super().__init__()

        def cluster_spec(self) -> Dict[str, Dict[str, int]]:
            return {
                "1": {
                    "clients": 10,
                    "batchers": 0,
                    "read_batchers": 0,
                    "leaders": 2,
                    "proxy_leaders": "leaders",
                    "acceptors": 3,
                    "replicas": 3,
                    "proxy_replicas": "replicas",
                }
            }

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
                    flexible = flexible,
                    distribution_scheme = DistributionScheme.COLOCATED,
                    client_jvm_heap_size = '8g',
                    batcher_jvm_heap_size = '12g',
                    read_batcher_jvm_heap_size = '12g',
                    leader_jvm_heap_size = '12g',
                    proxy_leader_jvm_heap_size = '12g',
                    acceptor_jvm_heap_size = '12g',
                    replica_jvm_heap_size = '12g',
                    proxy_replica_jvm_heap_size = '12g',
                    measurement_group_size = 10,
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

                # for num_batchers in [0]
                # for batch_size in [0]
                # for num_proxy_replicas in [0]
                # for proxy_leader_flush_every_n in [1]
                # for proxy_replica_flush_every_n in [1]
                #
                # for (flexible, num_acceptor_groups, num_acceptors_per_group) in [
                #     (True, 2, 2),
                #     (True, 3, 2),
                # ]
                # for leader_flush_every_n in [10, 5, 20]
                # for value_size in [1000, 100]
                # for num_replicas in [4, 5]
                # for num_proxy_leaders in [10, 13, 7]
                # for (num_client_procs, num_clients_per_proc) in [
                #     (2, 100),
                #     (5, 100),
                #     (8, 100),
                #     (10, 100),
                # ]

                # for value_size in [16, 100, 1000]
                for value_size in [16]
                for num_batchers in [0]
                for batch_size in [0]
                for num_proxy_replicas in [3]
                for proxy_leader_flush_every_n in [1]
                for proxy_replica_flush_every_n in [1]
                for num_replicas in [3]
                for num_proxy_leaders in [2]
                for (flexible, num_acceptor_groups, num_acceptors_per_group) in [
                    (False, 1, 3),
                ]
                for (num_client_procs, num_clients_per_proc, leader_flush_every_n) in [
                    (1, 1, 1),
                    (1, 10, 5),
                    (5, 10, 10),
                    (5, 20, 10),
                    (6, 50, 10),
                    # (6, 100, 10),
                    # (10, 100, 10),
                ]
            ]* 3

        def summary(self, input: Input, output: Output) -> str:
            return str({
                'f':
                    input.f,
                'value_size':
                    input.workload,
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
                                  'paxos_lt_scala') as dir:
        suite.run_suite(dir)


if __name__ == '__main__':
    main(get_parser().parse_args())
