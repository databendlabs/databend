#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

name="unload_csv_max_file_size"

path="/tmp/$name/"
# rm -r $path/*

stmt "drop stage if exists ${name};"
stmt "create stage ${name} url='fs://$path'"
stmt "copy into @${name} from (select * from numbers(100000000)) max_file_size=100000 file_format=(type=csv)"

cat $path/* | wc -l | sed 's/ //g'


if [[ "$OSTYPE" == "darwin"* ]]; then
		file_sizes=($(find "$path" -type f -exec stat -f "%z" {} + | sort -n -r))
else
		file_sizes=($(find "$path" -type f -exec stat -c "%s" {} + | sort -n -r))
fi
max_size=${file_sizes[1]}

if [ "$max_size" -gt 120000 ]; then
    echo "max_size is $max_size"
fi
stmt "drop stage if exists ${name};"
