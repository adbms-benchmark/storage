#! /usr/bin/python3

"""
Extract times from results csv files, and generate plots according 
to the given parameters.
"""

from matplotlib import pyplot as plt
from matplotlib import gridspec
from os import listdir
from os.path import isfile, join
import matplotlib
import argparse
import csv

ALL_LINES=-1
INVALID_FIELD=-1
COLORS = ['black', 'red', 'gold', 'green', 'blue', 'magenta', 'cyan', 'gray', 'darkorange', 'navy', 'violet', 'lime', 'pink']

def get_csv_fields(filepath, row_ind_begin, row_ind_end, data_label_field_ind, data_field_ind, data_label_input):
    """
    Get the data from a csv file, between the given row indices and from the given
    x and y columns.
    """
    data = []
    data_label = ""
    with open(filepath) as f_obj:
        reader = csv.reader(f_obj, delimiter=',', skipinitialspace=True)

        row_ind = 0
        for row in reader:
            if row_ind_begin == ALL_LINES or (row_ind >= row_ind_begin and row_ind <= row_ind_end):
                if len(row) == 0:
                    continue
                data_field = 0.0
                if data_label == "":
                    if data_label_field_ind != INVALID_FIELD:
                        data_label = row[data_label_field_ind]
                    else:
                        data_label = data_label_input
                if data_field_ind != INVALID_FIELD:
                    data_field = float(row[data_field_ind])
                data.append(data_field)
            row_ind += 1

    return (data, data_label)


def plot_data(files, lines, split, data_label_field_ind, data_field_ind, data_labels,
              xlabel, ylabel, title, xtick_labels, out_file, legend_title, xtick_legend):
    """
    Generate plot.
    """
    fontname = "cmr10"
    fontsize = 22
    font = {'family' : fontname,
            'size'   : 20}
    matplotlib.rc('font', **font)
    def correct_font(x, fontsize=fontsize):
        x.set_fontsize(fontsize)
        x.set_fontname(fontname)

    # load data into alldata
    alldata = []
    data_lbls = []
    maxdata = []
    mindata = []
    ind = 0
    for f in files:
        (row_ind_begin, row_ind_end) = lines[ind]
        (data, lbl) = get_csv_fields(f, row_ind_begin, row_ind_end, data_label_field_ind, data_field_ind, data_labels[ind])
        alldata.append(data)
        data_lbls.append(lbl)

        if ind == 0:
            for v in data:
                maxdata.append(v)
                mindata.append(v)
        else:
            for j in range(len(data)):
                if data[j] > maxdata[j]:
                    maxdata[j] = data[j]
                if data[j] < mindata[j]:
                    mindata[j] = data[j]
        ind += 1

    # find out highest and second highest values (ymax and ymax2), lowest (ymin) and lowest at same index as ymax (ymaxmin)
    ymax = 0
    ymaxmin = 100000000
    ymax2 = 0
    ymin = 1000000000
    for i in range(len(maxdata)):
        if maxdata[i] > ymax:
            ymax2 = ymax
            ymax = maxdata[i]
            ymaxmin = mindata[i]
        if maxdata[i] > ymax2 and maxdata[i] < ymax:
            ymax2 = maxdata[i]
        if mindata[i] < ymin:
            ymin = mindata[i]
    if ymaxmin < ymax2:
        ymaxmin = ymax

    # do not split if the two highest values are closer to each other than threshold %
    data_range = ymax - ymin
    data_range_ymax = ymax - ymax2
    threshold = 0.3
    if data_range_ymax / data_range < threshold:
        split = False

    # plot data
    ax1 = None
    ax2 = None
    if split:
        f, (ax0, ax1, ax2) = plt.subplots(3, 1, sharex=True, figsize=(8,12))
        f.delaxes(ax0)
    else:
        f, (ax1, ax2) = plt.subplots(2, 1, sharex=True, figsize=(8,10))
        f.delaxes(ax1)
        ax1 = plt
    for i in range(len(alldata)):
        ax1.plot(alldata[i], label=data_lbls[i], marker='^', lw=1.0, markersize=9, color=COLORS[i], linestyle='-')
        if split:
            ax2.plot(alldata[i], label=data_lbls[i], marker='^', lw=1.0, markersize=9, color=COLORS[i], linestyle='-')

    
    # set labels, title and legend
    correct_font(plt.xlabel(xlabel))
    correct_font(plt.ylabel(ylabel))
    if split:
        ax1 = plt.axes(ax1)
    if title:
        correct_font(plt.title(title), int(1.2 * fontsize))
    if split:
        ax2 = plt.axes(ax2)
    if xtick_legend:
        xtick_legend = xtick_legend.replace(";", "\n")
        yoffset = 0.72
        if not split:
            yoffset = 0.6
        correct_font(plt.figtext(0.04, yoffset, xtick_legend), 12)
    ax1.legend(loc='best', ncol=2, title=legend_title)
    plt.xticks(range(len(xtick_labels)), xtick_labels)
    ax1.grid(True)

    # split plot if requested
    if split:
        # zoom to outlier part on the top plot, and to the data on the bottom plot
        (ylow,yhigh) = ax1.get_ylim()
        ax1.set_ylim(ymaxmin * 0.9, ymax * 1.05)
        ax2.set_ylim(ymin * 0.95, ymax2 * 1.05)

        # hide the spines between ax1 and ax2
        ax1.spines['bottom'].set_visible(False)
        ax2.spines['top'].set_visible(False)
        ax1.xaxis.tick_top()
        ax1.tick_params(labeltop='off') # don't put tick labels at the top
        ax2.xaxis.tick_bottom()

        d = .015  # how big to make the diagonal lines in axes coordinates
        # arguments to pass to plot, just so we don't keep repeating them
        kwargs = dict(transform=ax1.transAxes, color='k', clip_on=False)
        ax1.plot((-d, +d), (-d, +d), **kwargs)        # top-left diagonal
        ax1.plot((1 - d, 1 + d), (-d, +d), **kwargs) # top-right diagonal
        kwargs.update(transform=ax2.transAxes)  # switch to the bottom axes
        ax2.plot((-d, +d), (1 - d, 1 + d), **kwargs)  # bottom-left diagonal
        ax2.plot((1 - d, 1 + d), (1 - d, 1 + d), **kwargs) # bottom-right diagonal
        ax2.grid(True)

    plt.tight_layout()
    if out_file:
        plt.savefig(out_file, bbox_inches='tight')
    else:
        plt.show()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("-f", "--files", help="comma-separated list of CSV files, e.g. file1,file2,...")
    parser.add_argument("-d", "--dir", help="load all .csv files from a specified directory.")
    parser.add_argument("-s", "--split", help="split plot in two for separately plotting the outlier.", action='store_true', default=False)
    parser.add_argument("--lines", help="comma-separated start-end line number for each file, e.g. 0-10,12-30,.. If the start-end are same for all files, only one can be specified.")
    parser.add_argument("--data-field", help="get data values for the plot from the given field in the CSV file (0-index).")
    parser.add_argument("--data-label-field", help="get labels for the legend from a column in the CSV file (0-index).", type=int, default=INVALID_FIELD)
    parser.add_argument("--data-labels", help="manually list the labels for the legend, separated by ','.")
    parser.add_argument("--xlabel", help="x axis label.")
    parser.add_argument("--ylabel", help="x axis label.", default="Processing time (ms)")
    parser.add_argument("--xtick-labels", help="custom tick labels for the X axis, comma-separated.")
    parser.add_argument("--xtick-legend", help="legend for the X axis ticks, as ';' separated strings.")
    parser.add_argument("--title", help="plot title.")
    parser.add_argument("--legend-title", help="legend title.")
    parser.add_argument("-o", "--outfile", help="file name for saving the plot.", default="plot.png")

    args = parser.parse_args()

    if not args.files and not args.dir:
        print("Please specify the files or a directory with benchmark results.")
    elif not args.data_field:
        print("Please specify the y field index.")
    else:
        files = []
        if args.dir:
            files = sorted([join(args.dir, f) for f in listdir(args.dir) if isfile(join(args.dir, f)) and f.endswith(".csv")])
        else:
            files = args.files.split(",")

        lines = []
        if not args.lines:
            for f in files:
                lines.append((ALL_LINES, ALL_LINES))
        else:
            tmp_lines = args.lines.split(",")
            for tmp_line in tmp_lines:
                line_list = tmp_line.split("-")
                lines.append((int(line_list[0]), int(line_list[1])))
            if len(tmp_lines) == 1:
                for i in range(len(files) - 1):
                    lines.append(lines[0])

        data_labels = []
        if args.data_labels:
            data_labels = args.data_labels.split(",")

        xtick_labels = []
        if args.xtick_labels:
            xtick_labels = args.xtick_labels.split(",")
        else:
            xtick_labels = ["Q" + str(i+1) for i in range(len(files))]

        plot_data(files, lines, args.split, args.data_label_field, int(args.data_field), 
            data_labels, args.xlabel, args.ylabel, args.title, xtick_labels, args.outfile, args.legend_title, args.xtick_legend)
