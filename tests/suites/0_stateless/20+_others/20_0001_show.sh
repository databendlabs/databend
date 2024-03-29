#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh



stmt "drop table if exists default.tc;"
stmt 'create table default.tc(id int, Id1_ int, "id2)" int, "ID4" int);'
stmt "set quoted_ident_case_sensitive=0;show create table default.tc"
stmt "set quoted_ident_case_sensitive=1;show create table default.tc"
stmt "set quoted_ident_case_sensitive=1;set sql_dialect='MySQL';show create table default.tc"
stmt "drop table default.tc;"
