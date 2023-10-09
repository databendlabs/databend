#!/bin/bash

echo "DROP STAGE IF EXISTS ss_temp" | ${BENDSQL}
echo "CREATE STAGE ss_temp" | ${BENDSQL}

echo "ABCD" >/tmp/a1.txt
echo "ABCD" >/tmp/a2.txt

echo 'put fs:///tmp/a*.txt @ss_temp/abc' | ${BENDSQL}
echo 'get @ss_temp/abc fs:///tmp/edf' | ${BENDSQL}
