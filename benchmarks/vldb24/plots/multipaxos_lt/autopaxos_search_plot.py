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

markers = ["o", "^", "s", "D", "p", "P", "X", "d", "v", "1", "2", "+", ">", "<"]
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
    if len(args.results) != 1:
        raise Exception('List exactly one results file')
    fig, ax = plt.subplots(1, 1, figsize=(6.4, 4.0))
    
    
    # Abbreviate fields.
    df = pd.read_csv(args.results[0])
    df['num_clients'] = df['num_client_procs'] * df['num_clients_per_proc']
    df['throughput'] = df['write_output.start_throughput_1s.p90'] if 'write_output.start_throughput_1s.p90' in df else df['start_throughput_1s.p90']
    df['latency'] = df['write_output.latency.median_ms'] if 'write_output.latency.median_ms' in df else df['latency.median_ms']

    grouped_df = df.groupby(['num_replicas', 'num_acceptor_partitions', 'num_p2a_proxy_leaders_per_leader', 'num_p2b_proxy_leaders_per_leader'])

    if len(grouped_df) > len(markers):
        raise Exception(f'List more matplotlib markers to match the number of results: {len(grouped_df)}')
    
    for (i, (fields, df)) in enumerate(grouped_df):
        label = f"AutoPaxos(Replicas={fields[0]}, Acc_Partitions={fields[1]}, P2a_PL={fields[2]}, P2b_PL={fields[3]})"
        if fields[0] != 3:
            continue
        plot_lt(df, ax, markers[i] + "-", label)

    ax.set_title('')
    ax.set_xlabel('Throughput (thousands of commands per second)')
    ax.set_ylabel('Median latency (ms)')
    ax.legend(loc='lower center', bbox_to_anchor=(0.5, 1))
    ax.grid()
    fig.savefig(args.output, bbox_inches='tight')
    print(f'Wrote plot to {args.output}.')


def get_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()

    parser.add_argument('--results',
                        type=argparse.FileType('r'),
                        help='results.csv file',
                        nargs='+')
    parser.add_argument('--output',
                        type=str,
                        default='autopaxos_search.pdf',
                        help='Output filename')

    return parser


if __name__ == '__main__':
    parser = get_parser()
    main(parser.parse_args())
