#!/bin/bash

DRIVER=driver.StorageBenchmark

java -Xmx8000m -Xms256m -cp lib/slf4j-api-1.7.10.jar:lib/slf4j-simple-1.7.10.jar:lib/JSAP-2.1.jar:lib/hsqldb.jar:lib/monetdb-jdbc-2.10.jar:lib/rasj.jar:build/classes $DRIVER $*
