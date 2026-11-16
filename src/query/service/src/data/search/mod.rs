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

//! `search`: ranks the rows of a table and returns the best of them, what the equivalent
//! SQL returns: `SELECT <output> FROM <table> WHERE match(..) ORDER BY score() DESC
//! LIMIT ..` for a text search, `.. ORDER BY cosine_distance(..) LIMIT ..` for a vector one.
//!
//! 1. A [`Retriever`], built from the [`RetrieverSpec`] of the request, finds the best
//!    rows of each block of the target.
//! 2. [`top_hits`] merges the blocks into the requested ranks. A retriever with
//!    approximate block scores first rescores the best hits of all blocks
//!    ([`Retriever::rerank`]), then [`rank`] ranks them.
//! 3. The [`RowFetcher`](crate::data::core::RowFetcher) of the core reads their output
//!    columns, and the score is added as [`SCORE_COLUMN`].
//!
//! A new way to rank rows is a `RetrieverSpec` variant and a file in [`retrievers`].

mod executor;
mod merge;
mod request;
mod retriever;
pub mod retrievers;

pub use executor::SearchOutput;
pub use executor::SearchStats;
pub use executor::execute;
pub use merge::RankedHit;
pub use merge::rank;
pub use merge::top_hits;
pub use request::MAX_SEARCH_ROWS;
pub use request::RetrieverSpec;
pub use request::SCORE_COLUMN;
pub use request::SearchRequest;
pub use retriever::BlockHit;
pub use retriever::Retriever;
pub use retriever::ScoreOrder;
pub use retrievers::text::TextOperator;
pub use retrievers::text::TextSpec;
pub use retrievers::vector::VectorDistance;
pub use retrievers::vector::VectorSpec;
