from .echo import *


def main(args) -> None:
    class SmokeEchoSuite(EchoSuite):
        def args(self) -> Dict[Any, Any]:
            return vars(args)

        def inputs(self) -> Collection[Input]:
            def gen_input(clients) -> Input:
                return Input(
                    num_clients_per_proc=clients,
                    jvm_heap_size='100m',
                    duration=datetime.timedelta(seconds=10),
                    timeout=datetime.timedelta(seconds=3),
                    warmup_duration=datetime.timedelta(seconds=3),
                    warmup_timeout=datetime.timedelta(seconds=3),
                    warmup_sleep=datetime.timedelta(seconds=1),
                    client_lag=datetime.timedelta(seconds=1),
                    profiled=args.profile,
                    monitored=args.monitor,
                    prometheus_scrape_interval=datetime.timedelta(
                        milliseconds=200),
                )

            return [
                gen_input(10),
                gen_input(25),
                gen_input(75),
                gen_input(100),
                gen_input(150),
                gen_input(200),
                gen_input(250),
                gen_input(350),
            ]

        def summary(self, input: Input, output: Output) -> str:
            return str({
                'num_clients_per_proc': input.num_clients_per_proc,
                'latency.median_ms': output.latency.median_ms,
                'start_throughput_1s.p90': output.start_throughput_1s.p90,
            })

    suite = SmokeEchoSuite()
    with benchmark.SuiteDirectory(args.suite_directory,
                                  'echo_smoke') as dir:
        suite.run_suite(dir)


if __name__ == '__main__':
    main(get_parser().parse_args())
