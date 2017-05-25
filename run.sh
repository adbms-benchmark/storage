#!/bin/bash

readonly MYNAME=$(basename $0)


unexpected_type_param() {
    echo "Unexpected argument: $1"
    echo "The first argument must be the benchmark type: 'storage', 'caching', 'sqlmda' or 'operations'."
    echo "Example: $MYNAME caching --help"
    exit 1
}

if [ -z "$1" ]; then
    unexpected_type_param ""
fi

driver=""
case "$1" in
    storage) driver="benchmark.storage.StorageBenchmarkDriver";;
    caching) driver="benchmark.caching.CachingBenchmarkDriver";;
    sqlmda)  driver="benchmark.sqlmda.SqlMdaBenchmarkDriver";;
    operations)  driver="benchmark.operations.OperationsBenchmarkDriver";;
    *) unexpected_type_param "$1";;
esac

# remove first arg
shift

JARS=""
for jar in lib/*.jar; do
    JARS="$jar:$JARS"
done

java -Xmx8000m -Xms256m -cp $JARS:build/classes $driver $*
