#!/bin/bash

cd /tpch-dbgen
./dbgen -vf -s $1
mv *.tbl /data
