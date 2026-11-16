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

//! Operations on the data of one table without planning SQL, served by `/v1/data/<op>`
//! for high-concurrency requests such as full-text search. An operation returns what its
//! equivalent SQL returns, for less work per request.
//!
//! - [`core`]: shared by every operation. Resolves the table and checks it readable like
//!   SQL does, lists the blocks of its latest snapshot, and reads columns of the rows an
//!   operation found.
//! - [`search`]: ranks the rows of each block with a retriever and merges the best of
//!   them, like `ORDER BY score() DESC LIMIT ..`.
//!
//! An operation is a module next to `search` that finds [`core::RowRef`]s and reads them
//! with [`core::RowFetcher`]; operations do not depend on each other.

pub mod core;
pub mod search;
