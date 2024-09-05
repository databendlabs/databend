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

//! This crate defines meta data types used by meta-client application, e.g. Schema, User, Share etc.
//! Such as Database, Table and User etc.
//!
//! Types in this crate will not be used directly by databend-meta.
//! But instead, they are used by the caller of meta-client, e.g, databend-query.

#![allow(clippy::uninlined_format_args)]
#![feature(no_sanitize)]

pub mod app_error;
pub mod background;
pub mod data_id;
pub mod data_mask;
pub mod primitive;
pub mod principal;
pub mod schema;
pub mod share;
pub mod storage;
pub mod tenant;
pub mod tenant_key;

pub mod id_generator;

mod key_with_tenant;
pub use key_with_tenant::KeyWithTenant;
