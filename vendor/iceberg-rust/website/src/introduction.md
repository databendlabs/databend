<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
-->

# Iceberg Rust

`iceberg-rust` is a Rust implementation for managing Apache Iceberg tables.

## What is Apache Iceberg?

[Apache Iceberg](https://iceberg.apache.org) is a modern, high-performance open table format
for huge analytic datasets that brings SQL-like tables to processing engines including Spark, Trino, PrestoDB, Flink, Hive and Impala. 

Iceberg provides a metadata layer that sits on top of formats like Parquet 
and ORC, ensuring data is organized, accessible, and safe to work with at scale. It introduces features long
expected in databases such as transactional consistency, schema evolution, and time travel into environments 
where files are stored directly on systems like Amazon S3.
