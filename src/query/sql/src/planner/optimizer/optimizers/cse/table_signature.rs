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

use std::collections::BTreeSet;
use std::collections::HashMap;

use crate::ColumnEntry;
use crate::IndexType;
use crate::optimizer::ir::SExpr;
use crate::planner::metadata::Metadata;
use crate::plans::RelOperator;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TableSignature {
    pub tables: BTreeSet<IndexType>,
}

pub fn collect_table_signatures(
    root: &SExpr,
    metadata: &Metadata,
) -> HashMap<TableSignature, Vec<(Vec<usize>, SExpr)>> {
    let mut signature_to_exprs = HashMap::new();
    let mut path = Vec::new();
    collect_table_signatures_rec(root, &mut path, metadata, &mut signature_to_exprs);
    signature_to_exprs
}

fn collect_table_signatures_rec(
    expr: &SExpr,
    path: &mut Vec<usize>,
    metadata: &Metadata,
    signature_to_exprs: &mut HashMap<TableSignature, Vec<(Vec<usize>, SExpr)>>,
) {
    for (child_index, child) in expr.children().enumerate() {
        path.push(child_index);
        collect_table_signatures_rec(child, path, metadata, signature_to_exprs);
        path.pop();
    }

    if let RelOperator::Scan(scan) = expr.plan.as_ref() {
        let has_internal_column = scan.columns.iter().any(|column_index| {
            let column = metadata.column(*column_index);
            matches!(column, ColumnEntry::InternalColumn(_))
        });
        if has_internal_column
            || scan.prewhere.is_some()
            || scan.agg_index.is_some()
            || scan.change_type.is_some()
            || scan.update_stream_columns
            || scan.inverted_index.is_some()
            || scan.vector_index.is_some()
            || scan.is_lazy_table
            || scan.sample.is_some()
        {
            return;
        }

        let table_entry = metadata.table(scan.table_index);
        let table = table_entry.table();
        if table.engine() != "FUSE" {
            return;
        }

        let mut tables = BTreeSet::new();
        tables.insert(table.get_table_id() as IndexType);
        signature_to_exprs
            .entry(TableSignature { tables })
            .or_default()
            .push((path.clone(), expr.clone()));
    }
}
