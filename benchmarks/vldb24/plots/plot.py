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
    grouped = df.groupby('num_clients')
    throughput = grouped['throughput'].agg(np.mean).sort_index() / 1000
    throughput_std = grouped['throughput'].agg(np.std).sort_index() / 1000
    latency = grouped['latency'].agg(np.mean).sort_index()
    line = ax.plot(throughput, latency, marker, label=label, linewidth=2)[0]
    ax.fill_betweenx(latency,
                     throughput - throughput_std,
                     throughput + throughput_std,
                     color = line.get_color(),
                     alpha=0.25)


def main(args) -> None:
    if len(args.results) != len(args.titles):
        raise Exception('Must have the same number of results and titles')
    if len(args.results) > len(markers):
        raise Exception('List more matplotlib markers to match the number of results')
    
    fig, ax = plt.subplots(1, 1, figsize=(8, 3))
    
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
    # ax.legend(loc='upper')
    ax.grid()
    fig.savefig(args.output, bbox_inches='tight')
    print(f'Wrote plot to {args.output}.')

    fig_leg = plt.figure(figsize=(len(args.titles)*3, 0.5))
    ax_leg = fig_leg.add_subplot(111)
    # add the legend from the previous axes
    ax_leg.legend(*ax.get_legend_handles_labels(), loc='center', ncol=len(args.titles))
    # hide the axes frame and the x/y labels
    ax_leg.axis('off')
    fig_leg.savefig('legend.pdf')


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
