# -*- coding: utf-8 -*-

import argparse
import os
import glob
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.ticker as tkr
import matplotlib.cm as cm
import numpy as np
import pandas as pd
from pylab import rcParams
import dir_utils


# Plotting parameters.
rcParams['figure.figsize'] = 30, 10
rcParams['font.size'] = 20
rcParams['axes.labelsize'] = 20
rcParams['legend.fontsize'] = 20
rcParams['ytick.labelsize'] = 15
rcParams['xtick.labelsize'] = 15
rcParams['ps.fonttype'] = 42
rcParams['pdf.fonttype'] = 42
rcParams['font.family'] = 'sans-serif'
rcParams['text.latex.preamble'] = r'\usepackage{sfmath}'


# Maximum latency (us) to include in the plots.
LATENCY_CLIP_US = 400
# Maximum experiment time (us) to include in the plots.
EXP_TIME_CLIP_US = 45 * 1000000


def fmt_us_to_s_(x, pos):
    """
    Converts us to s.
    """
    return "{:.0f}".format(x // 1000000)


def get_machine_(filepath):
    """
    Extract machine name from a given filepath.
    e.g.,
        filepath: storage_service__<machine>.traces
    """
    try:
        machine = filepath.split('.')[0].split("__")[1]
        return machine
    except Exception as e:
        print(e)
        return ''


def get_title_from_op_(op):
    """
    Get a plot title from an op keyword.
    Note: Camel-case the input op.
    """
    return op.title()


def get_trace_files(rootpath, category):
    """
    Extract filenames of all trace files from the given rootpath.
    """
    filepaths = glob.glob(f'{rootpath}/*.{category}.traces')
    filenames = [os.path.basename(fp) for fp in filepaths]
    print(f'Trace files: {filenames}')
    return filenames


def get_all_traces_df(filenames):
    """
    Read the given list of .csv trace files into a DataFrame.
    """
    dfs = []
    for filename in filenames:
        df = pd.read_csv(filename, header=0)
        if df is None:
            continue
        df['op_us'] = df['end_tsc'] - df['start_tsc']
        df['machine'] = get_machine_(filename)
        df.sort_values(by=['start_tsc'])
        # Machine-specific minimum start_tsc value
        min_start_tsc = df.iloc[0]['start_tsc']
        df['exp_us'] = df['start_tsc'] - min_start_tsc
        dfs.append(df)
    if len(dfs) == 0:
        print('No data frames to concat')
        return None
    df = pd.concat(dfs)
    return df


def get_cdf_values_by_op(df):
    """
    Gets a DataFrame and returns the values to plot.
    Returns a tuple of two tuples:
        ((read_x, read_y), (write_x, write_y))
    """
    # Reads.
    df_read = df[df['is_op_write'] == 0]
    read_times = df_read['op_us'].sort_values()
    read_x = read_times.values
    read_y = 100.0 * np.arange(read_x.shape[0]) / read_x.shape[0]
    # Writes.
    df_write = df[df['is_op_write'] == 1]
    write_times = df_write['op_us'].sort_values()
    write_x = write_times.values
    write_y = 100.0 * np.arange(write_x.shape[0]) / write_x.shape[0]
    return ((read_x, read_y), (write_x, write_y))


def get_cdf_values_agg(df):
    """
    Gets a DataFrame and returns the values to plot.
    Returns a tuple:
        (x, y)
    """
    # Reads.
    op_times = df['op_us'].sort_values()
    x = op_times.values
    y = 100.0 * np.arange(x.shape[0]) / x.shape[0]
    return (x, y)


def get_percentile_stats(x, y):
    stats = ""
    for i, p in enumerate([99, 99.9, 99.99]):
        if p <= 0:
            v = x[0]
        elif p >= 100:
            v = x[-1]
        else:
            v = x[y >= p][0]
        if i > 0:
            stats += ", "
        stats += f'P{p}: {v}$\\mu$s'
    return stats


def plot_latency_cdf(trace_paths, op, category):
    # Setup figure.
    figsize = (8, 6)
    linewidth = 2.0
    fig = plt.figure(figsize=figsize)
    ax = fig.gca()

    # Add gridlines.
    ax.grid(linestyle='-.', axis='y', linewidth=0.5, alpha=0.4)
    ax.grid(linestyle='-.', axis='x', linewidth=0.5, alpha=0.4)

    stats = ''
    for trace_path in trace_paths:
        # Parse traces.
        trace_files = get_trace_files(trace_path, category)
        with dir_utils.cd(trace_path):
            df = get_all_traces_df(trace_files)
        if op == 'read' or op == 'write':
            # Read traces.
            ((read_x, read_y), (write_x, write_y)) = get_cdf_values_by_op(df)
            # Plot.
            if op == 'read':
                ax.plot(
                        read_x,
                        read_y,
                        label=trace_path,
                        linewidth=linewidth,
                )
                stats += f'{trace_path} = {get_percentile_stats(read_x, read_y)}\n'
            elif op == 'write':
                ax.plot(
                        write_x,
                        write_y,
                        label=trace_path,
                        linewidth=linewidth,
                )
                stats += f'{trace_path} = {get_percentile_stats(write_x, write_y)}\n'
            else:
                pass
        elif op == 'aggregate':
            # Read traces.
            (x, y) = get_cdf_values_agg(df)
            # Plot.
            ax.plot(
                    x,
                    y,
                    label=trace_path,
                    linewidth=linewidth,
            )
            stats += f'{trace_path} = {get_percentile_stats(x, y)}\n'
        else:
            raise Exception(f'Unknown op: {op}')

    # Axis limits.
    ax.set_xlim(0, )
    ax.set_ylim(95, 100)
    ax.legend(loc='lower right', ncols=1)
    ax.set_title(f'{category.title()}: {get_title_from_op_(op)}')
    # plt.suptitle(get_title_from_op_(op), y=0.95)
    # plt.title(stats, fontsize=14, x=0.45)

    # Legends and axis labels.
    plt.xlabel('Latency ($\\mu$s)')
    fig.supylabel('CDF (%)')
    plt.tight_layout(pad=1)

    # Save.
    plt.savefig(f'cdf_{op}_{category}.pdf', format='pdf')


def get_args():
    """
    Get arguments.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--trace-paths', '-t',
                        dest='trace_paths',
                        nargs='+',
                        type=str,
                        required=True,
                        help='Path to directories of trace files to compare.')
    parser.add_argument('--op', '-o',
                        dest='op',
                        type=str,
                        required=True,
                        choices=['read', 'write', 'aggregate'],
                        help='Operation to plot.')
    parser.add_argument('--category', '-c',
                        dest='category',
                        type=str,
                        required=True,
                        choices=['client', 'server', 'client_nonetwork'],
                        help='Category to plot.')
    return parser.parse_args()


def main(args):
    trace_paths = args.trace_paths
    op = args.op
    category = args.category
    plot_latency_cdf(trace_paths, op, category)


if __name__ == '__main__':
    main(get_args())
