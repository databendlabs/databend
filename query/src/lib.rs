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

#![feature(hash_raw_entry)]
#![feature(core_intrinsics)]
#![feature(arbitrary_self_types)]
#![feature(bool_to_option)]
#![feature(generic_associated_types)]
#![feature(type_alias_impl_trait)]

pub mod api;
pub mod metrics;

pub use query_clusters as clusters;
pub use query_common as common;
pub use query_configs as configs;
pub use query_middle::catalogs;
pub use query_middle::databases;
pub use query_middle::functions;
pub use query_middle::interpreters;
pub use query_middle::optimizers;
pub use query_middle::pipelines;
pub use query_middle::servers;
pub use query_middle::sessions;
pub use query_middle::sql;
pub use query_middle::storages;
pub use query_middle::table_functions;
pub use query_users as users;
