#! /usr/bin/python3

"""
Extract times from results csv files, and generate plots according 
to the given parameters.
"""

from matplotlib import pyplot as plt
from os import listdir
from os.path import isfile, join
import matplotlib
import argparse
import csv

ALL_LINES=-1
INVALID_FIELD=-1

def get_csv_fields(filepath, row_ind_begin, row_ind_end, data_label_field_ind, data_field_ind, data_label_input):
    """
    Get the data from a csv file, between the given row indices and from the given
    x and y columns.
    """
    data = []
    data_label = ""
    with open(filepath) as f_obj:
        reader = csv.reader(f_obj, delimiter=',')

        row_ind = 0
        for row in reader:
            row_ind += 1
            if row_ind_begin == ALL_LINES or (row_ind >= row_ind_begin and row_ind <= row_ind_end):
                data_field = 0.0
                if data_label == "":
                    if data_label_field_ind != INVALID_FIELD:
                        data_label = row[data_label_field_ind]
                    else:
                        data_label = data_label_input
                if data_field_ind != INVALID_FIELD:
                    data_field = float(row[data_field_ind])
                data.append(data_field)

    return (data, data_label)


def plot_data(files, lines, multi, data_label_field_ind, data_field_ind, data_labels, xlabel, ylabel, title, xtick_labels, out_file, legend_title):
    """
    Generate plot.
    """
    fontname = 'cmr10'
    fontsize = 20
    def correct_font(x, fontsize=fontsize):
        x.set_fontname(fontname)
        x.set_fontsize(fontsize)

    plt.figure(figsize=(12,8))
    ind = 0
    for f in files:
        (row_ind_begin, row_ind_end) = lines[ind]
        (data, data_label) = get_csv_fields(f, row_ind_begin, row_ind_end, data_label_field_ind, data_field_ind, data_labels[ind])
        plt.plot(data, label=data_label, marker='x')
        ind += 1

    correct_font(plt.xlabel(xlabel))
    correct_font(plt.ylabel(ylabel))
    correct_font(plt.title(title), int(1.2 * fontsize))

    plt.xticks(range(len(xtick_labels)), xtick_labels)
    plt.legend(loc='best', ncol=2, title=legend_title)
    plt.tight_layout()
    plt.grid(True)
    if out_file is not None:
        plt.savefig(out_file)
    plt.show()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("-f", "--files", help="comma-separated list of CSV files, e.g. file1,file2,...")
    parser.add_argument("-d", "--dir", help="load all .csv files from a specified directory.")
    parser.add_argument("--lines", help="comma-separated start-end line number for each file, e.g. 0-10,12-30,.. If the start-end are same for all files, only one can be specified.")
    parser.add_argument("--multi", help="combine values from multiple files into a single plot.", action="store_true")
    parser.add_argument("--data-field", help="get data values for the plot from the given field in the CSV file (0-index).")
    parser.add_argument("--data-label-field", help="get labels for the legend from a column in the CSV file (0-index).", type=int, default=INVALID_FIELD)
    parser.add_argument("--data-labels", help="manually list the labels for the legend, separated by ','.")
    parser.add_argument("--xlabel", help="x axis label.")
    parser.add_argument("--ylabel", help="x axis label.", default="Execution time (ms)")
    parser.add_argument("--xtick-labels", help="custom tick labels for the X axis, comma-separated.")
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

        plot_data(files, lines, args.multi, args.data_label_field, int(args.data_field), 
            data_labels, args.xlabel, args.ylabel, args.title, xtick_labels, args.outfile, args.legend_title)
