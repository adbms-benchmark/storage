#!/bin/bash

DRIVER=driver.SciQL

java -Xmx8000m -Xms256m -cp lib/hsqldb.jar:lib/monetdb-jdbc-2.10.jar:lib/rasj.jar:build/classes $DRIVER
