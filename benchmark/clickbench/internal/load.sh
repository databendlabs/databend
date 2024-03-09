#!/bin/bash
# Used to test internal

echo "drop user if exists a" | bendsql
echo "drop role if exists role1" | bendsql
echo "create role if exists role1" | bendsql
echo "grant create database on *.* to role role1" | bendsql
echo "create user a identified by '123' with DEFAULT_ROLE='role1'";
echo "greant role role1 to a";
echo "drop database if exists test" | bendsql
echo "drop database if exists test2" | bendsql
echo "create database test" | bendsql -ua -p123
for((i=1;i<=10000;i++));
do
	echo "create table if not exists test.t_$i(id int comment 'tes\t', c2 string comment 'c2comment')" | bendsql -ua -p123
done

echo "create database test2" | bendsql -ua -p123
for((i=1;i<=1000;i++));
do
	echo "create table if not exists test2.t_$i(id int comment 'tes\t', c2 string comment 'c2comment')" | bendsql -ua -p123
done
