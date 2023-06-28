#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh


# Should be <root>/tests/data/

for force in 'false'  'true'
do
	for purge in 'false'  'true'
	do
		table="test_max_files_force_${force}_purge_${purge}"
		echo "drop table if exists ${table}" | $MYSQL_CLIENT_CONNECT
		echo "CREATE TABLE ${table} (
      id INT,
      c1 INT
    ) ENGINE=FUSE;" | $MYSQL_CLIENT_CONNECT
	done
done

gen_files() {
  rm -rf /tmp/00_0005
  cp -r "$CURDIR"/../../../data/00_0005 /tmp
}

echo "drop stage if exists s5;" | $MYSQL_CLIENT_CONNECT
echo "create stage s5 url = 'fs:///tmp/00_0005/data/' FILE_FORMAT = (type = PARQUET)" | $MYSQL_CLIENT_CONNECT


for force in 'false'  'true'
do
	for purge in 'false'  'true'
	do
	  gen_files
		echo "--- force = ${force}, purge = ${purge}"
	  echo "select count(*) from @s5" | $MYSQL_CLIENT_CONNECT
		for i in {1..3}
		do
			table="test_max_files_force_${force}_purge_${purge}"
			echo "copy into ${table} from (select * from @s5 t) max_files=2 force=${force} purge=${purge}" | $MYSQL_CLIENT_CONNECT
			echo "select count(*) from ${table}" | $MYSQL_CLIENT_CONNECT
		  remain=$(ls -1 /tmp/00_0005/data/ | wc -l |  sed 's/ //g')
			echo "remain ${remain} files"
		done
	done
done

for force in 'false'  'true'
do
	for purge in 'false'  'true'
	do
		echo "drop table if exists test_max_files_force_${force}_purge_${purge}" | $MYSQL_CLIENT_CONNECT
	done
done
