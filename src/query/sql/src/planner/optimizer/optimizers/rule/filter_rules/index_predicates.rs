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

use crate::plans::Filter;
use crate::plans::IndexPredicateChecker;
use crate::plans::ScalarExpr;
use crate::plans::Visitor;

/// Whether the index pruners evaluate every predicate of `filter` before the
/// blocks are read, so that the Sort or Limit above it may be pushed into the
/// Scan.
///
/// True when every predicate reads an index column: the matches or scores of
/// the inverted index, or the vector score. With `allow_vector_columns` and a
/// vector index, also when every predicate is deterministic and reads columns
/// of the table: the vector index pruner evaluates those per block before the
/// HNSW search. A volatile predicate (`d > rand()`) is excluded, as it runs
/// again after reading and could reject the selected rows.
pub(super) fn filter_evaluated_by_index_pruners(
    filter: &Filter,
    has_inverted_index: bool,
    has_vector_index: bool,
    allow_vector_columns: bool,
) -> bool {
    if !has_inverted_index && !has_vector_index {
        return false;
    }

    filter.predicates.iter().all(is_index_predicate)
        || (allow_vector_columns
            && has_vector_index
            && filter.predicates.iter().all(is_vector_column_predicate))
}

/// Reads index columns only, at least one.
fn is_index_predicate(predicate: &ScalarExpr) -> bool {
    let mut checker = IndexPredicateChecker::new(false);
    let _ = checker.visit(predicate);
    checker.valid() && checker.has_index_column()
}

/// Deterministic, on the columns of the table or the vector score, and not on the
/// matches or scores of an inverted index, which its pruner evaluates.
fn is_vector_column_predicate(predicate: &ScalarExpr) -> bool {
    let mut checker = IndexPredicateChecker::new(true);
    let _ = checker.visit(predicate);
    checker.valid() && !checker.has_search_column() && predicate.is_deterministic()
}
