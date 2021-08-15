// Copyright 2020 Datafuse Labs.
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

pub use common::Common;
pub use database::Database;
pub use table::Table;
pub use table::TablePtr;
pub use table_function::TableFunction;

pub use crate::catalogs::impls::database_catalog::DatabaseCatalog;

#[cfg(test)]
mod common_test;
#[cfg(test)]
mod tests;

mod common;
mod database;
pub(crate) mod local;
pub(crate) mod remote;
pub(crate) mod system;
mod table;
mod table_function;
