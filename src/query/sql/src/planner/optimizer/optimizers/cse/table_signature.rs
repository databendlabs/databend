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

use std::collections::HashMap;

use crate::ColumnEntry;
use crate::IndexType;
use crate::optimizer::ir::SExpr;
use crate::planner::metadata::Metadata;
use crate::plans::Join;
use crate::plans::JoinType;
use crate::plans::RelOperator;
use crate::plans::Scan;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TableSignature {
    pub tables: Vec<IndexType>,
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
) -> Option<Vec<IndexType>> {
    let mut child_tables = Vec::with_capacity(expr.arity());
    for (child_index, child) in expr.children().enumerate() {
        path.push(child_index);
        child_tables.push(collect_table_signatures_rec(
            child,
            path,
            metadata,
            signature_to_exprs,
        ));
        path.pop();
    }

    match expr.plan.as_ref() {
        RelOperator::Scan(scan) => {
            let table_id = scan_signature(scan, metadata)?;
            let tables = vec![table_id];
            signature_to_exprs
                .entry(TableSignature {
                    tables: tables.clone(),
                })
                .or_default()
                .push((path.clone(), expr.clone()));
            Some(tables)
        }
        RelOperator::Join(join)
            if is_supported_cross_join(join)
                && child_tables.len() == 2
                && child_tables[0].is_some()
                && child_tables[1].is_some() =>
        {
            let mut tables = child_tables[0].clone().unwrap();
            tables.extend(child_tables[1].clone().unwrap());
            // Preserve operand order so side-swapped cross joins do not share a
            // signature and get remapped positionally later.
            signature_to_exprs
                .entry(TableSignature {
                    tables: tables.clone(),
                })
                .or_default()
                .push((path.clone(), expr.clone()));
            Some(tables)
        }
        _ => None,
    }
}

fn scan_signature(scan: &Scan, metadata: &Metadata) -> Option<IndexType> {
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
        || scan.secure_predicates.is_some()
    {
        return None;
    }

    let table_entry = metadata.table(scan.table_index);
    let table = table_entry.table();
    if table.engine() != "FUSE" {
        return None;
    }

    Some(table.get_id() as IndexType)
}

fn is_supported_cross_join(join: &Join) -> bool {
    join.join_type == JoinType::Cross
        && join.equi_conditions.is_empty()
        && join.non_equi_conditions.is_empty()
        && join.marker_index.is_none()
        && !join.from_correlated_subquery
        && !join.need_hold_hash_table
        && !join.is_lateral
        && join.single_to_inner.is_none()
        && join.build_side_cache_info.is_none()
}
