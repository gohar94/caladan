# -*- coding: utf-8 -*-

import os
import glob
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.ticker as tkr
import matplotlib.cm as cm
import numpy as np
import pandas as pd
from pylab import rcParams


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


# Path containing .trace files to plot.
TRACES_DIR = "/proj/mit-aifm-PG0/girfan/storage_project/analysis"
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


def get_trace_files(rootpath):
    """
    Extract filenames of all trace files from the given rootpath.
    """
    filepaths = glob.glob(f"{rootpath}/*.traces")
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


def get_scatter_values(df):
    """
    Gets a DataFrame and returns the values to plot.
    Returns a tuple of two tuples:
        ((read_x, read_y), (write_x, write_y))
    """
    # Reads.
    df_read = df[df['is_op_write'] == 0]
    read_x = df_read['exp_us']
    read_y = df_read['op_us']
    # Writes.
    df_write = df[df['is_op_write'] == 1]
    write_x = df_write['exp_us']
    write_y = df_write['op_us']
    return ((read_x, read_y), (write_x, write_y))


def get_cdf_values(df):
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


def plot_latency_scatter():
    # Parse traces.
    trace_files = get_trace_files(TRACES_DIR)
    df = get_all_traces_df(trace_files)
    df_grouped_machine = df.groupby('machine')

    # Setup figure.
    num_plots = len(df_grouped_machine)
    figsize = (12, 6)
    dim_subplot_x = 1
    dim_subplot_y = num_plots
    fig = plt.figure(figsize=figsize)
    gs = matplotlib.gridspec.GridSpec(dim_subplot_y, dim_subplot_x, figure=fig)
    xfmt = tkr.FuncFormatter(fmt_us_to_s_)

    # Plot a single trace as a subplot.
    for i, (machine, df_machine) in enumerate(df_grouped_machine):
        # Add subplot.
        ax = fig.add_subplot(gs[i, 0])
        ax.xaxis.set_major_formatter(xfmt)
        # Add gridlines.
        ax.grid(linestyle='-.', axis='y', linewidth=0.5, alpha=0.4)
        ax.grid(linestyle='-.', axis='x', linewidth=0.5, alpha=0.4)
        # Read traces.
        ((read_x, read_y), (write_x, write_y)) = get_scatter_values(df_machine)
        # Plot.
        ax.scatter(
                read_x,
                read_y,
                label='Read',
                color='blue',
                alpha=0.5,
                s=20
        )
        ax.scatter(
                write_x,
                write_y,
                label='Write',
                color='green',
                alpha=0.5,
                s=20
        )
        # Axis limits.
        ax.set_xlim(0, EXP_TIME_CLIP_US)
        ax.set_ylim(0, LATENCY_CLIP_US)
        ax.set_title(machine)
        # Legend only on the top-most subplot.
        if i == 0:
            ax.legend(loc='upper left', ncols=2)
        else:
            ax.legend().set_visible(False)

    # Legends and axis labels.
    fig.supylabel('Latency ($\\mu$s)')
    plt.xlabel('Time (s)')
    plt.tight_layout(pad=1)

    # Save.
    plt.savefig(f'storage_server.pdf', format='pdf')


def plot_latency_cdf():
    # Parse traces.
    trace_files = get_trace_files(TRACES_DIR)
    df = get_all_traces_df(trace_files)
    df_grouped_machine = df.groupby('machine')

    # Setup figure.
    num_plots = len(df_grouped_machine)
    figsize = (8, 6)
    linewidth = 2.0
    dim_subplot_x = 1
    dim_subplot_y = num_plots
    fig = plt.figure(figsize=figsize)
    gs = matplotlib.gridspec.GridSpec(dim_subplot_y, dim_subplot_x, figure=fig)

    # Plot a single trace as a subplot.
    for i, (machine, df_machine) in enumerate(df_grouped_machine):
        # Add subplot.
        ax = fig.add_subplot(gs[i, 0])
        # Add gridlines.
        ax.grid(linestyle='-.', axis='y', linewidth=0.5, alpha=0.4)
        ax.grid(linestyle='-.', axis='x', linewidth=0.5, alpha=0.4)
        # Read traces.
        ((read_x, read_y), (write_x, write_y)) = get_cdf_values(df_machine)
        # Plot.
        ax.plot(
                read_x,
                read_y,
                label='Read',
                color='blue',
                linewidth=linewidth,
        )
        ax.plot(
                write_x,
                write_y,
                label='Write',
                color='green',
                linewidth=linewidth,
        )
        # Axis limits.
        ax.set_xlim(0, LATENCY_CLIP_US)
        ax.set_ylim(0, 100)
        ax.set_title(machine)
        # Legend only on the top-most subplot.
        if i == 0:
            ax.legend(loc='upper right', ncols=2)
        else:
            ax.legend().set_visible(False)

    # Legends and axis labels.
    plt.xlabel('Latency ($\\mu$s)')
    fig.supylabel('CDF (%)')
    plt.tight_layout(pad=1)

    # Save.
    plt.savefig(f'storage_server_cdf.pdf', format='pdf')


if __name__ == '__main__':
    plot_latency_cdf()
    plot_latency_scatter()
