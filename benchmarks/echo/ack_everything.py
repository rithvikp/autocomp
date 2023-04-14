from .echo import *

# hpython -m benchmarks.echo.ack_everything -j /mnt/nfs/tmp/frankenpaxos-assembly-0.1.0-SNAPSHOT.jar -m -s /mnt/nfs/tmp/ -l info --cluster_config ../clusters/echo/scala_hydro_config.json

# hpython -m benchmarks.echo.ack_everything -j /mnt/nfs/tmp/frankenpaxos-assembly-0.1.0-SNAPSHOT.jar -m -s /mnt/nfs/tmp/ -l info --cluster_config ../clusters/echo/dedalus_hydro_config.json

class AckEverythingEchoSuite(EchoSuite):
    def __init__(self, args, persist: bool) -> None:
        self._persist = persist
        self._args = args
        super().__init__()

    def args(self) -> Dict[Any, Any]:
        return vars(self._args)

    def cluster_spec(self) -> Dict[str, Dict[str, int]]:
        return {
            '1': {
                'servers': 1,
                'clients': 1,
            },
        }

    def inputs(self) -> Collection[Input]:
        def gen_input(clients) -> Input:
            return Input(
                num_clients_per_proc=clients,
                jvm_heap_size='100m',
                persist_log=self._persist,
                flush_every_n=1, # Do not change, flush_every_n does not work for this benchmark
                # duration=datetime.timedelta(seconds=60),
                duration=datetime.timedelta(seconds=10),
                timeout=datetime.timedelta(seconds=120),
                # warmup_duration=datetime.timedelta(seconds=15),
                warmup_duration=datetime.timedelta(seconds=5),
                warmup_timeout=datetime.timedelta(seconds=30),
                warmup_sleep=datetime.timedelta(seconds=1),
                # Need a large lag in order for Prometheus to initialize correctly
                client_lag=datetime.timedelta(seconds=10),
                profiled=self._args.profile,
                monitored=self._args.monitor,
                prometheus_scrape_interval=datetime.timedelta(
                    milliseconds=200),
            )

        return [
            # gen_input(1),
            # gen_input(10),
            # gen_input(25),
            # gen_input(50),
            # gen_input(75),
            gen_input(100),
            # gen_input(125),
            # gen_input(150),
            # gen_input(175),
            # gen_input(200),
        ]#*3

    def summary(self, input: Input, output: Output) -> str:
        return str({
            'num_clients_per_proc': input.num_clients_per_proc,
            'latency.median_ms': output.latency.median_ms,
            'start_throughput_1s.p90': output.start_throughput_1s.p90,
        })


def main(args) -> None:
    suite = AckEverythingEchoSuite(args, False)
    with benchmark.SuiteDirectory(args.suite_directory,
                                  'echo_ack_everything') as dir:
        suite.run_suite(dir)


if __name__ == '__main__':
    main(get_parser().parse_args())
