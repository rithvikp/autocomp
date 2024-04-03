
# import matplotlib
# matplotlib.use('pdf')
# font = {'size': 16}
# matplotlib.rc('font', **font)

from typing import Any, List
import matplotlib.pyplot as plt
import numpy as np
import os
import pandas as pd
import re

def output_plot(name, files):
    fig, ax = plt.subplots(1, 1, figsize=(2, 5)) # Skinny and tall
    barwidth = 0.1

    for i, file in enumerate(files):
        dfs = pd.read_csv(file)
        # Abbreviate fields.
        for df in [dfs]:
            df['num_clients'] = df['num_client_procs'] * df['num_clients_per_proc']
            df['throughput'] = df['write_output.start_throughput_1s.p90'] if 'write_output.start_throughput_1s.p90' in df else df['start_throughput_1s.p90']
        
        grouped = df.groupby('num_clients')
        throughputs = (grouped['throughput'].agg(np.mean).sort_index() / 1000).to_list()
        throughput_stds = (grouped['throughput'].agg(np.std).sort_index() / 1000).to_list()
        max_throughput_index = np.argmax(throughputs)
        plt.bar(i*barwidth, throughputs[max_throughput_index], barwidth, yerr=throughput_stds[max_throughput_index])
   
    plt.title(name)
    plt.ylabel('Throughput (thousands of commands per second)')
    plt.tick_params(axis='x', which='both', bottom=False, top=False, labelbottom=False)
    fig.savefig(name + "_throughput_only.pdf", bbox_inches='tight')
    


def main(args):
    twopc_files = ["twopc_lt/fixed_base.csv", "twopc_lt/fixed_auto5.csv"]
    paxos_files = ["multipaxos_lt/base.csv", "multipaxos_lt/scale3.csv"]
    pbft_files = ["pbft_critical_path_lt/results.csv", "pbft_critical_path_lt/auto3.csv"]

    output_plot("2PC", twopc_files)
    output_plot("Paxos", paxos_files)
    output_plot("PBFT", pbft_files)
    

if __name__ == '__main__':
    main([])