Array DBMS storage benchmark framework
======================================

This is a framework for benchmarking storage management capabilities in
Array Database Systems.

Supported systems
=================
Currently the following systems are supported:
* rasdaman (http://www.rasdaman.org/)
* SciDB (http://www.scidb.org/)
* SciQL (https://www.monetdb.org, unreleased branch SciQL-2 at http://dev.monetdb.org/hg/MonetDB/branches)

Getting started
===============
* `cp -r conf.in conf`, and adapt properties files in conf
* Build code: `ant jar`
* Execute with `./run.sh [OPTIONS]`
* Benchmark results are written in CSV format in `$HOME/results`.

Command-line usage help
-----------------------
`./run.sh --help`
```
Usage:
  classes [--help] (-s|--systems) system1,system2,...,systemN  --system-configs
  config1,config2,...,configN  (-d|--dimensions)
  dimension1,dimension2,...,dimensionN  (-b|--sizes) size1,size2,...,sizeN 
  (-r|--repeat) <repeat> (-q|--queries) <queries> --max-select-size
  <max_select_size> --timout <timeout> (-t|--tile-size) <tilesize> --datadir
  <datadir> [-c|--create] [--drop] [--disable-benchmark] [-v|--verbose]

Benchmark storage management in Array Databases. Currently supported systems:
rasdaman, SciDB, SciQL.


  [--help]
        Prints this help message.

  (-s|--systems) system1,system2,...,systemN 
        Array DBMS to target in this run. (default: rasdaman,scidb,sciql)

  --system-configs config1,config2,...,configN 
        System configuration (connection details, directories, etc). (default:
        conf/rasdaman.properties,conf/scidb.properties,conf/sciql.properties)

  (-d|--dimensions) dimension1,dimension2,...,dimensionN 
        Data dimensionality to be tested. (default: 1,2,3,4,5,6)

  (-b|--sizes) size1,size2,...,sizeN 
        Data sizes to be tested, as a number followed by B,kB,MB,GB,TB,PB,EB.
        (default: 1kB,100kB,1MB,100MB,1GB)

  (-r|--repeat) <repeat>
        Times to repeat each test query. (default: 5)

  (-q|--queries) <queries>
        Number of queries per query category. (default: 6)

  --max-select-size <max_select_size>
        Maximum select size, as percentage of the array size. (default: 10)

  --timout <timeout>
        Query timeout in seconds; -1 means no query timeout. (default: -1)

  (-t|--tile-size) <tilesize>
        Tile size, same format as for the --sizes option. (default: 4MB)

  --datadir <datadir>
        Data directory, for temporary and permanent data used in ingestion.
        (default: /tmp)

  [-c|--create]
        Create data.

  [--drop]
        Drop data.

  [--disable-benchmark]
        Do not run benchmark, just create or drop data.

  [-v|--verbose]
        Print extra information.
```
