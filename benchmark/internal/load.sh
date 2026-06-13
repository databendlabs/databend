#!/bin/bash
# Used to test internal

echo "drop database if exists test" | bendsql
echo "drop database if exists test2" | bendsql
echo "create database test" | bendsql
for((i=1;i<=5000;i++));
do
  if ((i%1000==0)); then
    echo "load $i tables in test"
  fi
	echo "create table if not exists test.t_$i(id int comment 'tes\t', c2 string comment 'c2comment')" | bendsql
done

echo "create database test2" | bendsql
for((i=1;i<=1000;i++));
do
	echo "create table if not exists test2.t_$i(id int comment 'tes\t', c2 string comment 'c2comment')" | bendsql
done

echo "create database test3" | bendsql
for((i=1;i<=1000;i++));
do
	echo "create table if not exists test3.t_$i(id int comment 'tes\t', c2 string comment 'c2comment')" | bendsql
done

for((i=1;i<=500;i++));
do
	echo "drop table if exists test3.t_$i;" | bendsql
done
