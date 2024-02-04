#!/bin/bash
# Used to test internal

echo "drop database if exists test" | bendsql -uroot
echo "create database test" | bendsql -uroot
for((i=1;i<=1000;i++));
do
	echo "create table if not exists test.t_$i(id int comment 'tes\t', c2 string comment 'c2comment')" | bendsql -uroot

done