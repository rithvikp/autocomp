from .echo import *


def main(args) -> None:
    class SmokeEchoSuite(EchoSuite):
        def args(self) -> Dict[Any, Any]:
            return vars(args)

        def inputs(self) -> Collection[Input]:
            return [
                Input(
                    num_clients_per_proc=10,
                    jvm_heap_size='100m',
                    duration=datetime.timedelta(seconds=10),
                    timeout=datetime.timedelta(seconds=3),
                    client_lag=datetime.timedelta(seconds=0),
                    profiled=args.profile,
                    monitored=args.monitor,
                    prometheus_scrape_interval=datetime.timedelta(
                        milliseconds=200),
                )
            ]

        def summary(self, input: Input, output: Output) -> str:
            return str({
                'num_clients_per_proc': input.num_clients_per_proc,
            })

    suite = SmokeEchoSuite()
    with benchmark.SuiteDirectory(args.suite_directory,
                                  'echo_smoke') as dir:
        suite.run_suite(dir)


if __name__ == '__main__':
    main(get_parser().parse_args())
