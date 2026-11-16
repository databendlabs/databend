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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use serde::Deserialize;

use crate::data::core::TableRef;
use crate::data::core::validate_output;
use crate::data::search::retrievers::text::TextSpec;
use crate::data::search::retrievers::vector::VectorSpec;

/// The name of the score column a search can output.
pub const SCORE_COLUMN: &str = "_score";

/// Upper bound of `offset + limit`: a search ranks this many rows of each block at most.
pub const MAX_SEARCH_ROWS: usize = 10_000;

/// One search: rank the rows of a table with one retriever and return the best of them.
/// The table fields are those of [`TableRef`].
#[derive(Deserialize, Debug, Clone, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SearchRequest {
    #[serde(default)]
    pub catalog: Option<String>,
    #[serde(default)]
    pub database: Option<String>,
    pub table: String,
    /// How the rows are ranked.
    pub retrieve: RetrieverSpec,
    /// The number of rows to return.
    pub limit: usize,
    /// The number of best ranked rows to skip.
    #[serde(default)]
    pub offset: usize,
    /// The columns to return, in order; [`SCORE_COLUMN`] is the score of the row.
    pub output: Vec<String>,
}

/// A way to rank rows. Each variant is served by one `Retriever` of
/// [`retrievers`](crate::data::search::retrievers).
#[derive(Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum RetrieverSpec {
    /// Full-text search: BM25 over an inverted index, like `match(..) ORDER BY score() DESC`.
    Text(TextSpec),
    /// Nearest neighbor search over a vector index, like
    /// `ORDER BY cosine_distance(..) LIMIT ..`.
    Vector(VectorSpec),
}

impl SearchRequest {
    pub fn table_ref(&self) -> TableRef<'_> {
        TableRef {
            catalog: self.catalog.as_deref(),
            database: self.database.as_deref(),
            table: &self.table,
        }
    }

    /// Rejects requests that are invalid whatever the table.
    pub fn validate(&self) -> Result<()> {
        if self.limit == 0 {
            return Err(ErrorCode::BadArguments("search limit must be positive"));
        }
        if self.offset.saturating_add(self.limit) > MAX_SEARCH_ROWS {
            return Err(ErrorCode::BadArguments(format!(
                "search offset + limit must not exceed {MAX_SEARCH_ROWS}"
            )));
        }
        validate_output("search", &self.output)?;
        match &self.retrieve {
            RetrieverSpec::Text(spec) => spec.validate(),
            RetrieverSpec::Vector(spec) => spec.validate(),
        }
    }

    /// The number of best rows of each block a retriever must find.
    pub fn candidates(&self) -> usize {
        self.offset + self.limit
    }
}
