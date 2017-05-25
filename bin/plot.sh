#!/bin/bash
set -u
# ----------------------------------------------------------------------------
# Description   Generate plots for all benchmarks.
# Dependencies  plot.py
#
# Date          2016-aug-22
# Author        Dimitar Misev
# ----------------------------------------------------------------------------

# ----------------------------------------------------------------------------
# variables
# ----------------------------------------------------------------------------

# script name
PROG=$(basename $0)

# return codes
readonly RC_OK=0    # everything went fine
readonly RC_ERROR=1 # something went wrong

# determine script directory
SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ] ; do SOURCE="$(readlink "$SOURCE")"; done
readonly SCRIPT_DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"

readonly PLOTPY="$SCRIPT_DIR/plot.py"
[ -f "$PLOTPY" ] || error "$PLOTPY not found."

# ----------------------------------------------------------------------------
# functions
# ----------------------------------------------------------------------------

# run if user hits Control-C
control_c()
{
  error "*** Signal caught ***"
}
 
# trap keyboard interrupt (Control-C)
trap control_c SIGINT

# logging
timestamp() {
  date +"%d-%m-%g %T"
}

error()
{
  echo >&2 [`timestamp`] $PROG: "$@"
  echo >&2 [`timestamp`] $PROG: exiting.
  exit $RC_ERROR
}

error_and_usage()
{
  echo >&2 [`timestamp`] $PROG: "$@"
  usage
}

check()
{
  if [ $? -ne 0 ]; then
    echo failed.
  else
    echo ok.
  fi
}

log()
{
  echo [`timestamp`] $PROG: "$@"
}

logn()
{
  echo -n [`timestamp`] $PROG: "$@"
}

usage()
{
  local -r usage="
Usage: $PROG [OPTION]...

Description..

Options:
  -h, --help
    display this help and exit
  --rasdir <DIR>
    specify directory with benchmark results for rasdaman
  --scidbdir <DIR>
    specify directory with benchmark results for SciDB
  --filter <FILE_NAME>
    specify benchmark session (output file name without the extension)
  --no-query-legend
    if specified, no query legend will be added to the plots
"

  echo "$usage"
  exit $RC_OK
}

# ----------------------------------------------------------------------------
# parse command-line options
# ----------------------------------------------------------------------------

option=""
rasdir=""
scidbdir=""
filter=""
no_query_legend=
for i in "$@"; do

  if [ -n "$option" ]; then
    case $option in
      --rasdir*)   rasdir="$i";;
      --scidbdir*) scidbdir="$i";;
      --filter*)   filter="$i";;
      *) error "unknown option: $option"
    esac
    option=""
    
  else
    option=""
    case $i in
      -h|--help*)     usage;;
      --no-query-legend*) no_query_legend=1;;
      *)              option="$i";;
    esac
  fi
  
done

[ -z "$rasdir" ] && error_and_usage "rasdaman results directory not specified."
[ ! -d "$rasdir" ] && error_and_usage "rasdaman results directory '$rasdir' not found."

mkdir -p rasdaman_graphs
[ -n "$scidbdir" -a ! -d "$scidbdir" ] && error_and_usage "SciDB results directory '$scidbdir' not found."
[ -d "$scidbdir" ] && mkdir -p scidb_rasdaman_comparison

# ----------------------------------------------------------------------------
# begin work
# ----------------------------------------------------------------------------

# build cache size info
cache_sizes_label=""
for f in "$rasdir"/*.csv; do
  cache_size=$(egrep "(rasdaman|SciDB|SciQL), " "$f" | head -n 1 | sed 's/.*, ".*", \([0-9]*\), .*/\1/')
  cache_size_gb=$(echo "scale=1; $cache_size/1000000000" | bc)
  [ -n "$cache_sizes_label" ] && cache_sizes_label="$cache_sizes_label,"
  cache_sizes_label="${cache_sizes_label}${cache_size_gb}"
done

readonly first_file=$(ls "$rasdir"/*.csv | head -n 1)

grep "# Benchmark session: " "$first_file" | sed 's/# Benchmark session: //' | \
while read bench_descr; do
  bench_descr_name="$(echo "$bench_descr" | tr ' ' '_' | tr -d ',')"
  if [ -n "$filter" ]; then
    [ "$bench_descr_name" == "$filter" ] || continue
  fi
  echo "plotting '$bench_descr' to '$bench_descr_name.png'"
  start_line=$(egrep -n "# Benchmark session: $bench_descr\$" "$first_file" | cut -f1 -d:)
  start_line=$(($start_line + 2))
  end_line_total=$(grep -n "'$bench_descr'" "$first_file" | cut -f1 -d:)
  end_line=$(($end_line_total - 1))
  total_lines=$(($end_line - $start_line + 1))


  # build xtick labels
  xtick_labels=""
  xtick_labels_descr=""
  line_no=$start_line
  i=1
  while [ $line_no -le $end_line ]; do
    [ -n "$xtick_labels" ] && xtick_labels="$xtick_labels,"
    [ -n "$xtick_labels_descr" ] && xtick_labels_descr="$xtick_labels_descr;"

    query=$(sed "${line_no}q;d" "$first_file" | sed 's/.*, "SELECT \(.*\) FROM .*", .*/\1/')
    xtick_labels_descr="${xtick_labels_descr}Q${i}: ${query}"
    xtick_labels="${xtick_labels}Q${i}"

    i=$(($i + 1))
    line_no=$(($line_no + 1))
  done

  if [ -n "$no_query_legend" ]; then
    python $PLOTPY -d "$rasdir" --data-field 4 --xlabel "Query" --lines $(($start_line-1))-$(($end_line-1)) \
                   --xtick-labels "$xtick_labels" \
                   --data-labels "$cache_sizes_label" -o rasdaman_graphs/"$bench_descr_name".pdf --split
  else
    python $PLOTPY -d "$rasdir" --data-field 4 --xlabel "Query" --lines $(($start_line-1))-$(($end_line-1)) \
                   --title "$bench_descr" --xtick-labels "$xtick_labels" --xtick-legend "$xtick_labels_descr" \
                   --data-labels "$cache_sizes_label" -o rasdaman_graphs/"$bench_descr_name".png --split
  fi

  if [ -d "$scidbdir" ]; then
    rm -f /tmp/rasdaman.csv; touch /tmp/rasdaman.csv
    rm -f /tmp/scidb.csv; touch /tmp/scidb.csv
    for f in "$rasdir"/*.csv; do
      sed "${end_line_total}q;d" "$f" | sed 's/.*: //' >> /tmp/rasdaman.csv
    done
    for f in "$scidbdir"/*.csv; do
      sed "${end_line_total}q;d" "$f" | sed 's/.*: //' >> /tmp/scidb.csv
    done

    # build xtick labels
    first_scidb_file=$(ls "$scidbdir"/*.csv | head -n 1)
    xtick_labels_descr=""
    line_no=$start_line
    i=1
    while [ $line_no -le $end_line ]; do
      [ -n "$xtick_labels_descr" ] && xtick_labels_descr="$xtick_labels_descr;"

      query_ras=$(sed "${line_no}q;d" "$first_file" | sed 's/.*, "SELECT \(.*\) FROM .*", .*/\1/')
      query_scidb=$(sed "${line_no}q;d" "$first_scidb_file" | sed 's/.*, "\(.*\)", .*/\1/' | tr -d ';')
      xtick_labels_descr="${xtick_labels_descr}Q${i}: ${query_ras}"
      both_queries="${query_ras}   ...   ${query_scidb}"
      both_queries_size=${#both_queries}
      if [ $both_queries_size -gt 130 ]; then
        xtick_labels_descr="${xtick_labels_descr};      ${query_scidb}"
      else
        xtick_labels_descr="${xtick_labels_descr}   ...   ${query_scidb}"
      fi

      i=$(($i + 1))
      line_no=$(($line_no + 1))
    done

    if [ -n "$no_query_legend" ]; then
      python $PLOTPY -f /tmp/rasdaman.csv,/tmp/scidb.csv --data-field 0 --xlabel "Cache size (GB)" \
                     --xtick-labels "$cache_sizes_label" --data-labels "rasdaman,scidb" \
                     -o scidb_rasdaman_comparison/"$bench_descr_name".pdf
    else
      python $PLOTPY -f /tmp/rasdaman.csv,/tmp/scidb.csv --data-field 0 --xlabel "Cache size (GB)" --title "$bench_descr" \
                     --xtick-legend "$xtick_labels_descr" --xtick-labels "$cache_sizes_label" --data-labels "rasdaman,scidb" \
                     -o scidb_rasdaman_comparison/"$bench_descr_name".png
    fi
  fi
  #break
done


# ----------------------------------------------------------------------------
# end work
# ----------------------------------------------------------------------------

log "done."
