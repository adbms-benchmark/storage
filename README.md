Array DBMS storage benchmark framework
======================================

This is a framework for benchmarking storage management capabilities in
Array Database Systems.

Supported systems
-----------------
Currently the following systems are supported:
* rasdaman (http://www.rasdaman.org/)
* SciDB (http://www.scidb.org/)
* SciQL (https://www.monetdb.org, unreleased branch SciQL-2 at http://dev.monetdb.org/hg/MonetDB/branches)
* ASQLDB (https://github.com/misev/asqldb)

Contributions for further systems are welcome.

Getting started
===============
* `cp -r conf.in conf`, and adapt properties files in conf
* `sudo cp drop_caches.sh /root && chown root: /root/drop_caches.sh`
 * `visudo`, and add a line `USER ALL=NOPASSWD: /root/drop_caches.sh`, where USER is the system user running the benchmark.
* Build code: `ant jar`
* Execute with `./run.sh [OPTIONS]`
* Benchmark results are written in CSV format in `$HOME/results`

Command-line usage help
-----------------------
`./run.sh TYPE --help`, where TYPE can be one of storage, caching or sqlmda.

E.g. `./run.sh caching --help`
```
Usage:
  run.sh [--help] (-s|--systems) system1,system2,...,systemN  --system-configs
  config1,config2,...,configN  --cache-sizes
  cacheSizes1,cacheSizes2,...,cacheSizesN  --datadir <datadir> [--load] [--drop]
  [--generate] [--disable-benchmark] [-v|--verbose]

Benchmark caching behaviour in Array Databases. Currently supported systems:
rasdaman, SciDB.


  [--help]
        Prints this help message.

  (-s|--systems) system1,system2,...,systemN 
        Array DBMS to target in this run. (default: rasdaman,scidb)

  --system-configs config1,config2,...,configN 
        System configuration (connection details, directories, etc). (default:
        conf/rasdaman.properties,conf/scidb.properties)

  --cache-sizes cacheSizes1,cacheSizes2,...,cacheSizesN 
        Cache sizes (in bytes) to benchmark. (default:
        1073741824,2147483648,3221225472,4294967296,8589934592)

  --datadir <datadir>
        Data directory, for temporary and permanent data used in ingestion.
        (default: /tmp)

  [--load]
        Load data.

  [--drop]
        Drop data.

  [--generate]
        Generate benchmark data.

  [--disable-benchmark]
        Do not run benchmark, just create or drop data.

  [-v|--verbose]
        Print extra information.
```

Copyright and license
=====================
Copyright (c) 2014-2016 George Merticariu, Dimitar Misev, Peter Baumann.

Code released under [the MIT license] (https://raw.githubusercontent.com/adbms-benchmark/storage/master/LICENSE).

