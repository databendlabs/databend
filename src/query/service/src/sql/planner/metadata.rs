// Copyright 2022 Datafuse Labs.
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

use common_ast::ast::Expr;
use common_ast::ast::Literal;
use common_datavalues::prelude::*;
use common_planner::ColumnEntry;
use common_planner::IndexType;

pub fn optimize_remove_count_args(name: &str, distinct: bool, args: &[&Expr]) -> bool {
    name.eq_ignore_ascii_case("count")
        && !distinct
        && args
            .iter()
            .all(|expr| matches!(expr, Expr::Literal{lit,..} if *lit!=Literal::Null))
}

pub fn find_smallest_column(entries: &[ColumnEntry]) -> usize {
    debug_assert!(!entries.is_empty());
    let mut column_indexes = entries
        .iter()
        .map(|entry| entry.index())
        .collect::<Vec<IndexType>>();
    column_indexes.sort();
    let mut smallest_index = column_indexes[0];
    let mut smallest_size = usize::MAX;
    for (idx, column_entry) in entries.iter().enumerate() {
        if let Ok(bytes) = column_entry.data_type().data_type_id().numeric_byte_size() {
            if smallest_size > bytes {
                smallest_size = bytes;
                smallest_index = entries[idx].index();
            }
        }
    }
    smallest_index
}
