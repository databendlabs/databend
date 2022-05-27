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
#![feature(generic_associated_types)]
#![feature(type_alias_impl_trait)]

extern crate core;

pub mod api;
pub mod catalogs;
pub mod clusters;
pub mod common;
pub mod databases;
pub mod formats;
pub mod interpreters;
pub mod metrics;
pub mod optimizers;
pub mod pipelines;
pub mod procedures;
pub mod servers;
pub mod sessions;
pub mod sql;
pub mod storages;
pub mod table_functions;
pub mod users;

mod config;
mod version;

pub use config::Config;
pub use version::DATABEND_COMMIT_VERSION;
