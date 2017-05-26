import os
#os.system("./run.sh operations -s scidb --system-configs conf/scidb.properties --datadir $(pwd)/data --load --generate --sizes 100MB -d 1,2,3,4,5 --datatype char")
#os.system("./run.sh operations -s scidb --system-configs conf/scidb.properties --datadir $(pwd)/data --load --generate --sizes 100MB -d 1,2,3,4,5 --datatype double")
#os.system("./run.sh operations -s scidb --system-configs conf/scidb.properties --datadir $(pwd)/data --load --generate --sizes 100MB -d 1,2,3,4,5 --datatype int32")
os.system("./run.sh operations -s scidb --system-configs conf/scidb.properties --datadir $(pwd)/data --load --generate --sizes 1GB -d 1,2,3,4,5 --datatype char")
os.system("./run.sh operations -s scidb --system-configs conf/scidb.properties --datadir $(pwd)/data --load --generate --sizes 1GB -d 1,2,3,4,5 --datatype double")
os.system("./run.sh operations -s scidb --system-configs conf/scidb.properties --datadir $(pwd)/data --load --generate --sizes 1GB -d 1,2,3,4,5 --datatype int32")
