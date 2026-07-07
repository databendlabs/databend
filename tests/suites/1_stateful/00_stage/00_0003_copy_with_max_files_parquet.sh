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
		echo "drop table if exists ${table}" | bendsql_connect_root
		echo "CREATE TABLE ${table} (
      id INT,
      c1 INT
    ) ENGINE=FUSE;" | bendsql_connect_root
	done
done

gen_files() {
  rm -rf $TMP
  mkdir $TMP
  cp  "$TESTS_DATA_DIR"/parquet/ii/* $TMP
}

echo "drop stage if exists s5;" | bendsql_connect_root
echo "create stage s5 url = 'fs://$TMP/' FILE_FORMAT = (type = PARQUET)" | bendsql_connect_root


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
			copied=$(echo "copy into ${table} from (select * from @s5 t) max_files=2 force=${force} purge=${purge}" | bendsql_connect_root | wc -l  |  sed 's/ //g')
			echo "copied ${copied} files"
		  remain=$(ls -1 $TMP | wc -l |  sed 's/ //g')
			echo "remain ${remain} files"
			remain=$(echo "select count(*) from ${table}" | bendsql_connect_root)
			echo "remain ${remain} rows"
		done
	done
done

for force in 'false'  'true'
do
	for purge in 'false'  'true'
	do
		echo "drop table if exists test_max_files_force_${force}_purge_${purge}" | bendsql_connect_root
	done
done
