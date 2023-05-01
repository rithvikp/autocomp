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


def avg_tput(df):
    return df['throughput'].agg(np.mean)


def std_tput(df):
    return df['throughput'].agg(np.std)


def add_num_clients(df: pd.DataFrame) -> pd.DataFrame:
    df['num_clients'] = df['num_client_procs'] * df['num_clients_per_proc']
    return df


def barchart(output_filename: str, labels: List[str], axis_labels: List[str], data: List[float],
             yerr: List[float], colors: List[str]) -> None:
    fig, ax = plt.subplots(1, 1, figsize=(6.4, 4.0))
    y_pos = range(len(data)+1)
    labels = labels[:3] + [''] + labels[3:]
    data = data[:3] + [0] + data[3:]
    colors = colors[:3] + [None] + colors[3:]
    yerr = yerr[:3] + [0] + yerr[3:]

    for i in y_pos:
        if i == 3:
            continue
        ax.barh(i, data[i], xerr=yerr[i], align='center', capsize=10, color=colors[i], label=labels[i])


    ax.set_yticks(y_pos)
    ax.set_yticklabels(labels)
    ax.set_title('')
    ax.set_xlabel('Scaling Factor')
    ax.set_ylabel('')
    ax.invert_yaxis()
    ax.set_xlim(1, ax.get_xlim()[1])

    # box = ax.get_position()
    # ax.set_position([box.x0, box.y0, box.width * 0.8, box.height])
    # ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))

    fig.savefig(output_filename, bbox_inches='tight')
    print(f'Wrote plot to {output_filename}.')


def main(args) -> None:
    df = add_num_clients(pd.read_csv(args.results))
    df = df[df['num_clients'] != 1]
    df['throughput'] = df['start_throughput_1s.p90']
    df['latency'] = df['latency.median_ms']

    data = []
    benchmarks = [
        "DECOUPLING_FUNCTIONAL",
        "DECOUPLING_MONOTONIC",
        "DECOUPLING_MUTUALLY_INDEPENDENT",
        "PARTITIONING_COHASHING",
        "PARTITIONING_DEPENDENCIES",
        "PARTITIONING_PARTIAL"
    ]

    for benchmark in benchmarks:
        regular_type = "MicrobenchmarkType." + benchmark
        auto_type = "MicrobenchmarkType.AUTO_" + benchmark
        regular_throughput = df[df['microbenchmark_type'] == regular_type]['throughput'].agg(np.mean)

        auto_df = df[df['microbenchmark_type'] == auto_type][['throughput', 'latency']]
        assert len(auto_df) == 3

        auto_df['scaling'] = auto_df['throughput'] / regular_throughput
        data.append([benchmark, auto_df['scaling'].agg(np.mean), auto_df['scaling'].agg(np.std)])

    # npl = comp_df['num_proxy_leaders']
    # na = comp_df['num_acceptor_groups'] * comp_df['num_acceptors_per_group']
    # nr = comp_df['num_replicas']

    barchart(
        output_filename = args.output,
        labels = [
            'Functional Decoupling',
            'Monotonic Decoupling',
            'Mutually Independent Decoupling',
            'Partitioning With Co-Hashing',
            'Partitioning With Dependencies',
            'Partial Partitioning'
        ],
        axis_labels = [
            'Functional',
            'Monotonic',
            'Mutually Independent',
            'Co-Hashing',
            'Dependencies',
            'Partial'
        ],
        data = [d[1] for d in data],
        yerr = [d[2] for d in data],
        colors = ['C0', 'C1', 'C2', 'C3', 'C4', 'C5']
    )


    # dfs = [
    #     coupled_df,
    #     comp_df[(npl == 2) & (na == 3) & (nr == 2)],
    #     comp_df[(npl == 3) & (na == 3) & (nr == 2)],
    #     comp_df[(npl == 4) & (na == 3) & (nr == 2)],
    #     comp_df[(npl == 5) & (na == 3) & (nr == 2)],
    #     comp_df[(npl == 6) & (na == 3) & (nr == 2)],
    #     comp_df[(npl == 7) & (na == 3) & (nr == 2)],
    #     comp_df[(npl == 7) & (na == 3) & (nr == 3)],
    #     comp_df[(npl == 8) & (na == 3) & (nr == 3)],
    #     comp_df[(npl == 9) & (na == 3) & (nr == 3)],
    #     comp_df[(npl == 10) & (na == 3) & (nr == 3)],
    # ]
    # barchart(
    #     output_filename=args.output,
    #     labels=[
    #         'coupled',
    #         'decoupled',
    #         '3 proxy leaders',
    #         '4 proxy leaders',
    #         '5 proxy leaders',
    #         '6 proxy leaders',
    #         '7 proxy leaders',
    #         '3 replicas',
    #         '8 proxy leaders',
    #         '9 proxy leaders',
    #         '10 proxy leaders',
    #     ],
    #     data=[avg_tput(df) / 1000 for df in dfs],
    #     yerr=[std_tput(df) / 1000 for df in dfs],
    #     color=['C0', 'C1', 'C2', 'C2', 'C2', 'C2', 'C2', 'C3', 'C2', 'C2', 'C2'],
    # )


def get_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument('--results',
                        type=argparse.FileType('r'),
                        default='combined.csv',
                        help='Microbenchmarks results.csv file')
    parser.add_argument(
        '--output',
        type=str,
        default='microbenchmarks.pdf',
        help='Output filename')
    return parser


if __name__ == '__main__':
    parser = get_parser()
    main(parser.parse_args())
