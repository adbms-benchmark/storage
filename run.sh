#!/bin/bash

DRIVER=driver.Driver

JARS=""
for jar in lib/*.jar; do
    JARS="$jar:$JARS"
done

java -Xmx8000m -Xms256m -cp $JARS:build/classes $DRIVER $*
