//  Copyright 2022 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

pub mod fuse_source;
mod native_data_source;
mod native_data_source_deserializer;
mod native_data_source_reader;
mod parquet_data_source;
mod parquet_data_source_deserializer;
mod parquet_data_source_reader;

pub use fuse_source::build_fuse_parquet_source_pipeline;
