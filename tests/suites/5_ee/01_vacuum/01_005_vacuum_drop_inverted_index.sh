#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh


### vacuum from single table

stmt "create or replace database test_vacuum_drop_inverted_index"

mkdir -p /tmp/test_vacuum_drop_inverted_index/

stmt "create or replace table test_vacuum_drop_inverted_index.books(id int, title string, author string, description string) 'fs:///tmp/test_vacuum_drop_inverted_index/'"

stmt "CREATE OR REPLACE INVERTED INDEX idx2 ON test_vacuum_drop_inverted_index.books(title, author, description) tokenizer = 'chinese' filters = 'english_stop,english_stemmer,chinese_stop'"

stmt "INSERT INTO test_vacuum_drop_inverted_index.books VALUES
(1, '这就是ChatGPT', '[美]斯蒂芬·沃尔弗拉姆（Stephen Wolfram）', 'ChatGPT是OpenAI开发的人工智能聊天机器人程序，于2022年11月推出。它能够自动生成一些表面上看起来像人类写的文字，这是一件很厉害且出乎大家意料的事。那么，它是如何做到的呢？又为何能做到呢？本书会大致介绍ChatGPT的内部机制，然后探讨一下为什么它能很好地生成我们认为有意义的文本。')"


SNAPSHOT_LOCATION=$(echo "select snapshot_location from fuse_snapshot('test_vacuum_drop_inverted_index','books') limit 1" | $BENDSQL_CLIENT_CONNECT)
PREFIX=$(echo "$SNAPSHOT_LOCATION" | cut -d'/' -f1-2)

echo "before vacuum, should be 1 index dir"

ls  /tmp/test_vacuum_drop_inverted_index/"$PREFIX"/_i_i/ | wc -l

stmt "drop inverted index idx2 on test_vacuum_drop_inverted_index.books"

stmt "set data_retention_time_in_days=0; select * from fuse_vacuum_drop_inverted_index('test_vacuum_drop_inverted_index','books')" > /dev/null

echo "after vacuum, should be 0 index dir"
find /tmp/test_vacuum_drop_inverted_index/"$PREFIX"/_i_i/ -type f | wc -l


## vacuum from all tables

echo "--------------------------vacuum from all tables--------------------------"

stmt "create or replace table test_vacuum_drop_inverted_index.book_1(id int, title string, author string, description string) 'fs:///tmp/test_vacuum_drop_inverted_index/'"

stmt "CREATE OR REPLACE INVERTED INDEX idx3 ON test_vacuum_drop_inverted_index.book_1(title) tokenizer = 'chinese' filters = 'english_stop,english_stemmer,chinese_stop'"
stmt "CREATE OR REPLACE INVERTED INDEX idx4 ON test_vacuum_drop_inverted_index.book_1(author, description) tokenizer = 'chinese' filters = 'english_stop,english_stemmer,chinese_stop'"

stmt "insert into test_vacuum_drop_inverted_index.book_1 values (1, '这就是ChatGPT', '[美]斯蒂芬·沃尔弗拉姆（Stephen Wolfram）', 'ChatGPT是OpenAI开发的人工智能聊天机器人程序，于2022年11月推出。它能够自动生成一些表面上看起来像人类写的文字，这是一件很厉害且出乎大家意料的事。那么，它是如何做到的呢？又为何能做到呢？本书会大致介绍ChatGPT的内部机制，然后探讨一下为什么它能很好地生成我们认为有意义的文本。')"

SNAPSHOT_LOCATION_1=$(echo "select snapshot_location from fuse_snapshot('test_vacuum_drop_inverted_index','book_1') limit 1" | $BENDSQL_CLIENT_CONNECT)
PREFIX_1=$(echo "$SNAPSHOT_LOCATION_1" | cut -d'/' -f1-2)

echo "before vacuum, should be 2 index dir"

find /tmp/test_vacuum_drop_inverted_index/"$PREFIX_1"/_i_i/ -type f | wc -l

stmt "create or replace table test_vacuum_drop_inverted_index.book_2(id int, title string, author string, description string) 'fs:///tmp/test_vacuum_drop_inverted_index/'"

stmt "CREATE OR REPLACE INVERTED INDEX idx5 ON test_vacuum_drop_inverted_index.book_2(title, author, description) tokenizer = 'chinese' filters = 'english_stop,english_stemmer,chinese_stop'"

stmt "insert into test_vacuum_drop_inverted_index.book_2 values (1, '这就是ChatGPT', '[美]斯蒂芬·沃尔弗拉姆（Stephen Wolfram）', 'ChatGPT是OpenAI开发的人工智能聊天机器人程序，于2022年11月推出。它能够自动生成一些表面上看起来像人类写的文字，这是一件很厉害且出乎大家意料的事。那么，它是如何做到的呢？又为何能做到呢？本书会大致介绍ChatGPT的内部机制，然后探讨一下为什么它能很好地生成我们认为有意义的文本。')"


SNAPSHOT_LOCATION_2=$(echo "select snapshot_location from fuse_snapshot('test_vacuum_drop_inverted_index','book_2') limit 1" | $BENDSQL_CLIENT_CONNECT)
PREFIX_2=$(echo "$SNAPSHOT_LOCATION_2" | cut -d'/' -f1-2)

stmt "drop inverted index idx3 on test_vacuum_drop_inverted_index.book_1"
stmt "drop inverted index idx4 on test_vacuum_drop_inverted_index.book_1"
stmt "drop inverted index idx5 on test_vacuum_drop_inverted_index.book_2"

echo "before vacuum, should be 1 index dir"

find /tmp/test_vacuum_drop_inverted_index/"$PREFIX_2"/_i_i/ -type f | wc -l

stmt "set data_retention_time_in_days=0; select * from fuse_vacuum_drop_inverted_index()" > /dev/null

echo "after vacuum, should be 0 index dir"

find /tmp/test_vacuum_drop_inverted_index/"$PREFIX_1"/_i_i/ -type f | wc -l
find /tmp/test_vacuum_drop_inverted_index/"$PREFIX_2"/_i_i/ -type f | wc -l


### create or replace index
echo "--------------------------create or replace index--------------------------"

stmt "create or replace database test_vacuum_drop_inverted_index"

mkdir -p /tmp/test_vacuum_drop_inverted_index/

stmt "create or replace table test_vacuum_drop_inverted_index.books(id int, title string, author string, description string) 'fs:///tmp/test_vacuum_drop_inverted_index/'"

stmt "CREATE OR REPLACE INVERTED INDEX idx2 ON test_vacuum_drop_inverted_index.books(title) tokenizer = 'chinese' filters = 'english_stop,english_stemmer,chinese_stop'"

stmt "INSERT INTO test_vacuum_drop_inverted_index.books VALUES
(1, '这就是ChatGPT', '[美]斯蒂芬·沃尔弗拉姆（Stephen Wolfram）', 'ChatGPT是OpenAI开发的人工智能聊天机器人程序，于2022年11月推出。它能够自动生成一些表面上看起来像人类写的文字，这是一件很厉害且出乎大家意料的事。那么，它是如何做到的呢？又为何能做到呢？本书会大致介绍ChatGPT的内部机制，然后探讨一下为什么它能很好地生成我们认为有意义的文本。')"


SNAPSHOT_LOCATION=$(echo "select snapshot_location from fuse_snapshot('test_vacuum_drop_inverted_index','books') limit 1" | $BENDSQL_CLIENT_CONNECT)
PREFIX=$(echo "$SNAPSHOT_LOCATION" | cut -d'/' -f1-2)

echo "before create or replace index, should be 1 index dir"

ls  /tmp/test_vacuum_drop_inverted_index/"$PREFIX"/_i_i/ | wc -l

stmt "CREATE OR REPLACE INVERTED INDEX idx2 ON test_vacuum_drop_inverted_index.books(author, description) tokenizer = 'chinese' filters = 'english_stop,english_stemmer,chinese_stop'"

stmt "set data_retention_time_in_days=0; select * from fuse_vacuum_drop_inverted_index('test_vacuum_drop_inverted_index','books')" > /dev/null

echo "after vacuum, should be 0 index dir"
find /tmp/test_vacuum_drop_inverted_index/"$PREFIX"/_i_i/ -type f | wc -l

