#!/bin/bash
# Used to test internal

echo "drop database if exists test" | bendsql -uroot
echo "drop database if exists test2" | bendsql -uroot
echo "create database test" | bendsql -uroot
for((i=1;i<=10000;i++));
do
	echo "create table if not exists test.t_$i(id int comment 'tes\t', c2 string comment 'c2comment')" | bendsql -uroot
done

echo "create database test2" | bendsql -uroot
for((i=1;i<=1000;i++));
do
	echo "create table if not exists test2.t_$i(id int comment 'tes\t', c2 string comment 'c2comment')" | bendsql -uroot
done
