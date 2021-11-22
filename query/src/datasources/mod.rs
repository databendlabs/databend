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

#[cfg(test)]
mod database_engine_registry_test;

mod database_engine;
mod database_engine_registry;
mod index;
mod table_engine;
mod table_engine_registry;
mod table_func_engine_registry;

pub(crate) mod common;
pub(crate) mod context;
pub(crate) mod database;
pub(crate) mod table;
pub(crate) mod table_func;
pub(crate) mod table_func_engine;

pub use database_engine::DatabaseEngine;
pub use database_engine_registry::DatabaseEngineRegistry;
pub use table_engine::TableEngine;
pub use table_engine_registry::TableEngineRegistry;
pub use table_func::prelude::prelude_func_engines;
pub use table_func_engine::TableArgs;
pub use table_func_engine::TableFuncEngine;
pub use table_func_engine_registry::TableFuncEngineRegistry;
