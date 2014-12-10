#!/bin/bash

DRIVER=driver.AsqldbSciQL

java -cp lib/hsqldb.jar:lib/monetdb-jdbc-2.10.jar:lib/rasj.jar:build/classes $DRIVER
