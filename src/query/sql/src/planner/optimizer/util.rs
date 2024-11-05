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

use super::SExpr;
use crate::plans::RelOperator;
use crate::MetadataRef;

/// Check if a query will read data from local tables(e.g. system tables).
pub fn contains_local_table_scan(s_expr: &SExpr, metadata: &MetadataRef) -> bool {
    s_expr
        .children()
        .any(|s_expr| contains_local_table_scan(s_expr, metadata))
        || if let RelOperator::Scan(get) = s_expr.plan() {
            metadata.read().table(get.table_index).table().is_local()
        } else {
            false
        }
        || matches!(s_expr.plan(), RelOperator::RecursiveCteScan { .. })
}
