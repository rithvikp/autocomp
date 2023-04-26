# See https://stackoverflow.com/a/19521297/3187068
import matplotlib
matplotlib.use('pdf')
font = {'size': 16}
matplotlib.rc('font', **font)

from typing import Any, List
import argparse
import matplotlib.pyplot as plt
import numpy as np
import os
import pandas as pd
import re

markers = ["o", "^", "s", "D", "p", "P", "X", "d"]

def plot_lt(df: pd.DataFrame, ax: plt.Axes, marker: str, label: str) -> None:
    throughput = df['throughput']
    latency = df['latency']
    line = ax.plot(throughput, latency, marker, label=label, linewidth=2)[0]


def main(args) -> None:
    if len(args.results) != len(args.titles):
        raise Exception('Must have the same number of results and titles')
    if len(args.results) > len(markers):
        raise Exception('List more matplotlib markers to match the number of results')
    
    fig, ax = plt.subplots(1, 1, figsize=(6.4, 4.0))
    
    for i, (result, title) in enumerate(zip(args.results, args.titles)):
        dfs = pd.read_csv(result)

        # Abbreviate fields.
        for df in [dfs]:
            df['num_clients'] = df['num_client_procs'] * df['num_clients_per_proc']
            df['throughput'] = df['write_output.start_throughput_1s.p90'] if 'write_output.start_throughput_1s.p90' in df else df['start_throughput_1s.p90']
            df['latency'] = df['write_output.latency.median_ms'] if 'write_output.latency.median_ms' in df else df['latency.median_ms']

        plot_lt(dfs, ax, markers[i] + "-", title)

    ax.set_title('')
    ax.set_xlabel('Throughput (thousands of commands per second)')
    ax.set_ylabel('Median latency (ms)')
    ax.legend(loc='lower left')
    ax.grid()
    fig.savefig(args.output, bbox_inches='tight')
    print(f'Wrote plot to {args.output}.')


def get_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()

    parser.add_argument('--results',
                        type=argparse.FileType('r'),
                        help='results.csv file',
                        nargs='+')
    parser.add_argument('--titles',
                        type=str,
                        help='Titles for each experiment',
                        nargs='+')
    parser.add_argument('--output',
                        type=str,
                        default='compartmentalized_lt.pdf',
                        help='Output filename')

    return parser


if __name__ == '__main__':
    parser = get_parser()
    main(parser.parse_args())
