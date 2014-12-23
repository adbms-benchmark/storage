#!/bin/bash

DRIVER=driver.AsqldbSciQL

java -Xmx4096m -Xms256m -cp lib/hsqldb.jar:lib/monetdb-jdbc-2.10.jar:lib/rasj.jar:build/classes $DRIVER
