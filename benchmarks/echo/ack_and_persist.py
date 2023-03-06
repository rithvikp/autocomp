from .echo import *
from .ack_everything import AckEverythingEchoSuite


def main(args) -> None:
    suite = AckEverythingEchoSuite(True)
    with benchmark.SuiteDirectory(args.suite_directory,
                                  'echo_ack_and_persist') as dir:
        suite.run_suite(dir)


if __name__ == '__main__':
    main(get_parser().parse_args())