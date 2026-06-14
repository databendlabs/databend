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

mod prepare;
mod query_rewrite;
mod rewrite;

use std::collections::HashMap;

use databend_common_expression::ColumnId;
use databend_common_expression::TableField;
use prepare::RangeClasses;
pub use query_rewrite::build_agg_index_plan_for_table;
pub use query_rewrite::try_rewrite;

use crate::IndexType;
use crate::ScalarExpr;
use crate::Symbol;
use crate::plans::Aggregate;
use crate::plans::ScalarItem;
use crate::plans::SortItem;

#[derive(Debug)]
struct QueryInfo {
    equi_classes: EquivalenceClasses,
    range_classes: RangeClasses,
    residual_classes: ResidualClasses,
    sort_items: Option<Vec<SortItem>>,
    aggregate: Option<Aggregate>,
    column_map: HashMap<Symbol, ScalarExpr>,
    column_identities: HashMap<Symbol, ColumnIdentity>,
    column_exprs: Vec<ColumnExprEntry>,
    output_cols: Vec<ScalarItem>,
}

#[derive(Debug)]
pub struct AggIndexViewInfo {
    query_info: QueryInfo,
    index_fields: Vec<TableField>,
    index_output_cols: Vec<IndexOutputColumn>,
}

#[derive(Debug, Default)]
struct EquivalenceClasses {
    classes: Vec<Vec<ScalarExpr>>,
}

#[derive(Debug, Default)]
struct ResidualClasses {
    residual_preds: Vec<ScalarExpr>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ColumnIdentity {
    table_index: IndexType,
    column_id: ColumnId,
    path_indices: Option<Vec<usize>>,
}

#[derive(Clone, Copy)]
struct ScalarMatchContext<'a> {
    column_map: &'a HashMap<Symbol, ScalarExpr>,
    column_identities: &'a HashMap<Symbol, ColumnIdentity>,
}

#[derive(Debug)]
struct ColumnExprEntry {
    expr: ScalarExpr,
    index: Symbol,
}

#[derive(Debug)]
struct IndexOutputColumn {
    expr: ScalarExpr,
    index_scalar: ScalarExpr,
    is_agg: bool,
}

struct ScalarExprMatcher<'a, 'b> {
    left: ScalarMatchContext<'a>,
    right: ScalarMatchContext<'b>,
}
