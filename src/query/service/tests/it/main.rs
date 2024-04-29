// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(clippy::uninlined_format_args)]
#![feature(thread_local)]
#![feature(int_roundings)]
#![allow(clippy::diverging_sub_expression)]
#![feature(assert_matches)]

extern crate core;
mod auth;
mod catalogs;
mod clusters;
mod configs;
mod databases;
mod distributed;
mod frame;
mod interpreters;
mod metrics;
mod parquet_rs;
mod pipelines;
mod servers;
mod sessions;
mod spillers;
mod sql;
mod storages;
mod table_functions;
mod tests;
