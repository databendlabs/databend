#!/bin/bash
# Copyright 2020-2021 The Databend Authors.
# SPDX-License-Identifier: Apache-2.0.
export DATASET_SHA256=429c47cdd49b7d75eec9b9244c8b1288edd132506203e37d2d1e70e1ac1eccc7

echo "Starting standalone databend cluster through bendctl"
killall databend-query
killall databend-meta
make cluster
if [ $? -eq 0 ]; then
	echo "provisioned cluster"
else
	echo "cannot provision cluster"
	exit 1
fi
if [[ -f "$HOME/dataset/ontime.csv" ]]; then
	echo "dataset exists"
else
	echo "dataset not exists."
	mkdir -p $HOME/dataset
	wget -P $HOME/dataset "https://repo.databend.rs/dataset/stateful/ontime.csv"
fi
echo "$DATASET_SHA256 $HOME/dataset/ontime.csv" >checksums.txt
sha256sum -c checksums.txt
if [ $? -eq 0 ]; then
	echo "dataset checksum passed"
else
	echo "current dataset is not our stateful test dataset"
	exit 1
fi

./target/release/bendctl --databend_dir ./.databend --group local query ./tests/suites/1_stateful/ontime/create_table.sql
if [ $? -eq 0 ]; then
	echo "dataset table DDL passed"
else
	echo "cannot create DDL"
	exit 1
fi
./target/release/bendctl --databend_dir ./.databend --group local load $HOME/dataset/ontime.csv --table ontime
if [ $? -eq 0 ]; then
	echo "successfull loaded ontime dataset"
else
	exit 1
fi
echo "Starting stateful databend-test"

# Create table
# shellcheck disable=SC2044
for i in $(find ./tests/suites/1_stateful/ctl -name "*.sql" -type f); do
	$BENDCTL --databend_dir ./.databend --group local query $i 2>$i.error >$i.out
	# shellcheck disable=SC2046
	if [ $(cat $i.error | wc -l) -ne 0 ]; then
		cat $i.error
		exit 1
	fi
	# shellcheck disable=SC2046
	if [ $(comm -13 <(sort -u $i.out) <(sort -u $i.result) | wc -l) -ne 0 ]; then
		echo "result does not match"
		echo $i.out
		exit 1
	fi
	echo $i passed
done
