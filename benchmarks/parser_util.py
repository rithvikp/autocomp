from . import pd_util
from typing import Tuple
import argparse
import os
import pandas as pd


def get_benchmark_parser() -> argparse.ArgumentParser:
    """
    get_benchmark_parser returns an argument parser with the flags most
    commonly used by benchmark scripts.
    """
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-s',
                        '--suite_directory',
                        type=str,
                        default='/tmp',
                        help='Benchmark suite directory')
    parser.add_argument(
        '-j',
        '--jar',
        type=str,
        default=os.path.join(
            os.path.dirname(os.path.realpath(__file__)), '..',
            'jvm/target/scala-2.12/frankenpaxos-assembly-0.1.0-SNAPSHOT.jar'),
        help='FrankenPaxos JAR file')
    parser.add_argument('-l',
                        '--log_level',
                        choices=['debug', 'info', 'warn', 'error', 'fatal'],
                        default='debug',
                        help='Log level')
    parser.add_argument('-p',
                        '--profile',
                        action='store_true',
                        help='Profile code using perf')
    parser.add_argument('-m',
                        '--monitor',
                        action='store_true',
                        help='Monitor code using prometheus')
    parser.add_argument('-a',
                        '--address',
                        action='append',
                        help='Remote addresses for benchmark')
    parser.add_argument('--cluster',
                        type=str,
                        default=None,
                        help='A JSON file with cluster information')
    parser.add_argument('-i',
                        '--identity_file',
                        help='SSH identity file for remote benchmarks')

    parser.add_argument('--cluster_spec',
                        type=str,
                        default=None,
                        help='A JSON file with a cluster specification to be constructed') 
    parser.add_argument('--cluster_config',
                        type=str,
                        default=None,
                        help='A JSON file with cloud-specific cluster configuration') 
    return parser


def get_plot_parser(default_output_filename: str) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument('results',
                        type=argparse.FileType('r'),
                        help='results.csv file')
    parser.add_argument('-o',
                        '--output',
                        type=str,
                        default=default_output_filename,
                        help='Output filename')
    return parser
