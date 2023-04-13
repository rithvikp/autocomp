from .voting import *

class SmokeVotingSuite(VotingSuite):
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
        def gen_input(clients: int, replicas: int) -> Input:
            return Input(
                num_clients_per_proc=clients,
                num_replicas=replicas,
                jvm_heap_size='100m',
                duration=datetime.timedelta(seconds=10),
                timeout=datetime.timedelta(seconds=120),
                warmup_duration=datetime.timedelta(seconds=5),
                warmup_timeout=datetime.timedelta(seconds=20),
                warmup_sleep=datetime.timedelta(seconds=1),
                # Need a large lag in order for Prometheus to initialize correctly
                client_lag=datetime.timedelta(seconds=10),
                log_level=self.args()['log_level'],
                leader_flush_every_n=1,
                profiled=self._args.profile,
                monitored=self._args.monitor,
                prometheus_scrape_interval=datetime.timedelta(
                    milliseconds=200),
            )

        return [
            # gen_input(10, 5),
            gen_input(100, 7),
        ]

        # return [
        #     gen_input(10),
        #     gen_input(25),
        #     gen_input(75),
        #     gen_input(100),
        #     gen_input(150),
        #     gen_input(200),
        #     gen_input(250),
        #     gen_input(350),
        # ]*3

    def summary(self, input: Input, output: Output) -> str:
        return str({
            'num_clients_per_proc': input.num_clients_per_proc,
            'num_replicas': input.num_replicas,
            'latency.median_ms': output.latency.median_ms,
            'start_throughput_1s.p90': output.start_throughput_1s.p90,
        })


def main(args) -> None:
    suite = SmokeVotingSuite(args)
    with benchmark.SuiteDirectory(args.suite_directory,
                                  'voting_smoke') as dir:
        suite.run_suite(dir)


if __name__ == '__main__':
    main(get_parser().parse_args())