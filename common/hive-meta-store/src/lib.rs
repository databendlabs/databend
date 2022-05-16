// Copyright 2021 Datafuse Labs.
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

#![allow(clippy::all)]
#![allow(dead_code)]
#![allow(unreachable_patterns)]
#![allow(unused_imports)]
#![allow(unused_variables)]
#![allow(unused_extern_crates)]
#![allow(deprecated)]
#![allow(clippy::too_many_arguments, clippy::type_complexity, clippy::vec_box)]
#![cfg_attr(rustfmt, rustfmt_skip)]
mod hive_meta_store;

pub use hive_meta_store::TThriftHiveMetastoreSyncClient;
pub use hive_meta_store::ThriftHiveMetastoreSyncClient;
pub use hive_meta_store::*;
pub use thrift;
