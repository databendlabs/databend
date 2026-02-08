#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

run_bendsql() {
  $BENDSQL_CLIENT_CONNECT <<SQL
$1
SQL
}

# Should be <root>/tests/data/

for transform in 'false'  'true'
do
	for force in 'false'  'true'
	do
		for purge in 'false'  'true'
		do
			table="test_max_files_force_${force}_purge_${purge}_transform_${transform}"
			run_bendsql "
DROP TABLE IF EXISTS ${table};
CREATE TABLE ${table} (
				id INT,
				c1 INT
			) ENGINE=FUSE;
"
		done
	done
done

rm -rf /tmp/00_0004
mkdir -p /tmp/00_0004

gen_files() {
cat << EOF > /tmp/00_0004/f1.csv
1,1
2,2
EOF

cat << EOF > /tmp/00_0004/f2.csv
3,3
4,4
EOF

cat << EOF > /tmp/00_0004/f3.csv
5,5
6,6
EOF
}

for transform in 'false'  'true'
do
	for force in 'false'  'true'
	do
		for purge in 'false'  'true'
		do
			gen_files
			echo "--- force = ${force}, purge = ${purge}, transform = ${transform}"
			for i in {1..3}
			do
				table="test_max_files_force_${force}_purge_${purge}_transform_${transform}"
				if [ "$transform" = true ]; then
					copied=$(echo "copy into ${table} from 'fs:///tmp/00_0004/' FILE_FORMAT = (type = CSV) max_files=2 force=${force} purge=${purge}" | $BENDSQL_CLIENT_CONNECT | wc -l |  sed 's/ //g')
        else
					copied=$(echo "copy into ${table} from (select \$1, \$2 from 'fs:///tmp/00_0004/') FILE_FORMAT = (type = CSV) max_files=2 force=${force} purge=${purge}" | $BENDSQL_CLIENT_CONNECT | wc -l |  sed 's/ //g')
        fi
				copied_rows=$(echo "select count(*) from ${table}" | $BENDSQL_CLIENT_CONNECT)
				remain=$(ls -1 /tmp/00_0004/ | wc -l |  sed 's/ //g')
				echo "copied ${copied} files with ${copied_rows} rows, remain ${remain} files"
			done
		done
	done
done

for transform in 'false'  'true'
do
	for force in 'false'  'true'
	do
		for purge in 'false'  'true'
		do
			table="test_max_files_force_${force}_purge_${purge}_transform_${transform}"
			run_bendsql "
DROP TABLE IF EXISTS ${table};
"
		done
	done
done


DIR=/tmp/00_0004_2
TABLE=test_max_files_limit
stmt "drop table if exists ${TABLE}"
stmt "create table ${TABLE} (a int, b int)"

rm -rf ${DIR}
mkdir ${DIR}
for i in {1..15001}
do
	echo "${i},1" > ${DIR}/f${i}.csv
done

stmt "copy into ${TABLE} from 'fs://${DIR}/' FILE_FORMAT = (type = CSV)"
stmt "copy into ${TABLE} from 'fs://${DIR}/' FILE_FORMAT = (type = CSV) force=true"
query "copy into ${TABLE} from 'fs://${DIR}/' FILE_FORMAT = (type = CSV) force=true purge=true return_failed_only=true"
query "drop table ${TABLE}"
