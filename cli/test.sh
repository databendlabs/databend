#!/bin/sh

port=8000
extra=""
if [ $1 = "flight" ];then
	port=8900
	extra="--flight"
fi

cargo build --bin bendsql

for f in `ls cli/tests/*.sql`;do
	echo "Testing -- ${f}"
	suite=`echo $f | sed -e 's#cli/tests/##'  | sed -e 's#.sql##'`
	cat $f | ./target/debug/bendsql -u ${DATABEND_USER} -p ${DATABEND_PASSWORD} --port ${port} -h${DATABEND_HOST} ${extra} > /tmp/${suite}.output 2>&1
	diff /tmp/${suite}.output cli/tests/${suite}.result
	
	ret_code=$?
	if [ $ret_code -ne 0 ]; then
		exit 1
	fi
done

rm /tmp/*.output

echo "Tests $1 passed"
