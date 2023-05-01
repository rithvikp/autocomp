import json
from benchmarks.echo.echo import *

# hpython -m benchmarks.echo.ack_everything -j /mnt/nfs/tmp/frankenpaxos-assembly-0.1.0-SNAPSHOT.jar -m -s /mnt/nfs/tmp/ -l info --cluster_config ../clusters/echo/scala_hydro_config.json

# hpython -m benchmarks.echo.ack_everything -j /mnt/nfs/tmp/frankenpaxos-assembly-0.1.0-SNAPSHOT.jar -m -s /mnt/nfs/tmp/ -l info --cluster_config ../clusters/echo/dedalus_hydro_config.json


def main(args) -> None:
    class PingSuite(EchoSuite):
        def __init__(self, args) -> None:
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
            return [
                Input(
                    num_clients_per_proc=clients,
                    jvm_heap_size='8g',
                    persist_log=False,
                    flush_every_n=1, # Do not change, flush_every_n does not work for this benchmark
                    duration=datetime.timedelta(seconds=60),
                    timeout=datetime.timedelta(seconds=65),
                    warmup_duration=datetime.timedelta(seconds=25),
                    warmup_timeout=datetime.timedelta(seconds=30),
                    warmup_sleep=datetime.timedelta(seconds=5),
                    # Need a large lag in order for Prometheus to initialize correctly
                    client_lag=datetime.timedelta(seconds=10),
                    profiled=self._args.profile,
                    monitored=self._args.monitor,
                    prometheus_scrape_interval=datetime.timedelta(
                        milliseconds=200),
                )
                for clients in [1]
            ]

        def summary(self, input: Input, output: Output) -> str:
            return str({
                'num_clients_per_proc': input.num_clients_per_proc,
                'latency.median_ms': output.latency.median_ms,
                'start_throughput_1s.p90': output.start_throughput_1s.p90,
            })

    suite = PingSuite(args)
    with benchmark.SuiteDirectory(args.suite_directory,
                                  'ping') as dir:
        suite.run_suite(dir)


if __name__ == '__main__':
    main(get_parser().parse_args())
