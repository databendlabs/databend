#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh


# Should be <root>/tests/data/
TMP="/tmp/00_0005"

for force in 'false'  'true'
do
	for purge in 'false'  'true'
	do
		table="test_max_files_force_${force}_purge_${purge}"
		echo "drop table if exists ${table}" | $BENDSQL_CLIENT_CONNECT
		echo "CREATE TABLE ${table} (
      id INT,
      c1 INT
    ) ENGINE=FUSE;" | $BENDSQL_CLIENT_CONNECT
	done
done

gen_files() {
  rm -rf $TMP
  mkdir $TMP
  cp  "$TESTS_DATA_DIR"/parquet/ii/* $TMP
}

echo "drop stage if exists s5;" | $BENDSQL_CLIENT_CONNECT
echo "create stage s5 url = 'fs://$TMP/' FILE_FORMAT = (type = PARQUET)" | $BENDSQL_CLIENT_CONNECT


for force in 'false'  'true'
do
	for purge in 'false'  'true'
	do
	  gen_files
		echo "--- force = ${force}, purge = ${purge}"
		for i in {1..3}
		do
			echo "--- --- 'copy max_files=2' ${i}"
			table="test_max_files_force_${force}_purge_${purge}"
			copied=$(echo "copy into ${table} from (select * from @s5 t) max_files=2 force=${force} purge=${purge}" | $BENDSQL_CLIENT_CONNECT | wc -l  |  sed 's/ //g')
			echo "copied ${copied} files"
		  remain=$(ls -1 $TMP | wc -l |  sed 's/ //g')
			echo "remain ${remain} files"
			remain=$(echo "select count(*) from ${table}" | $BENDSQL_CLIENT_CONNECT)
			echo "remain ${remain} rows"
		done
	done
done

for force in 'false'  'true'
do
	for purge in 'false'  'true'
	do
		echo "drop table if exists test_max_files_force_${force}_purge_${purge}" | $BENDSQL_CLIENT_CONNECT
	done
done
