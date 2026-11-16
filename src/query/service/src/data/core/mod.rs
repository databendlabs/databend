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

//! The parts shared by every data operation, which know nothing of scores or keys:
//!
//! 1. [`TableTarget`] resolves the [`TableRef`] of a request, checks it readable like SQL
//!    does and lists the blocks of its latest snapshot.
//! 2. [`for_each_block`] runs a per-block step over those blocks, in parallel on the
//!    runtime SQL queries prune on.
//! 3. [`RowFetcher`] reads the output columns of the [`RowRef`]s an operation found.
//!
//! [`run`] executes an operation on the query runtime. [`TableCache`] lets requests reuse a
//! recently resolved table, by the `data_api_table_cache_ttl_ms` setting.

mod fetch;
mod request;
mod row;
mod runtime;
mod table_cache;
mod target;

pub use fetch::RowFetcher;
pub use request::TableRef;
pub use request::validate_output;
pub use row::RowRef;
pub use runtime::for_each_block;
pub use runtime::run;
pub use table_cache::TableCache;
pub use table_cache::TableKey;
pub use target::TableTarget;
