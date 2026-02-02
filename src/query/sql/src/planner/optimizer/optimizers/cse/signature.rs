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
pub struct SExprSignature {
    pub tables: BTreeSet<IndexType>,
}

pub fn compute_s_expr_signatures(
    root: &SExpr,
    metadata: &Metadata,
) -> HashMap<SExprSignature, Vec<(Vec<usize>, SExpr)>> {
    let mut signature_to_exprs = HashMap::new();
    let mut path = Vec::new();
    compute_s_expr_signature_rec(root, &mut path, metadata, &mut signature_to_exprs);
    signature_to_exprs
}

fn compute_s_expr_signature_rec(
    expr: &SExpr,
    path: &mut Vec<usize>,
    metadata: &Metadata,
    signature_to_exprs: &mut HashMap<SExprSignature, Vec<(Vec<usize>, SExpr)>>,
) {
    for (child_index, child) in expr.children().enumerate() {
        path.push(child_index);
        compute_s_expr_signature_rec(child, path, metadata, signature_to_exprs);
        path.pop();
    }

    match expr.plan.as_ref() {
        RelOperator::Scan(scan) => {
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
            tables.insert(table.get_id() as IndexType);
            signature_to_exprs
                .entry(SExprSignature { tables })
                .or_default()
                .push((path.clone(), expr.clone()));
        }
        RelOperator::Join(join) => {
            if join.from_correlated_subquery || join.is_lateral {
                return;
            }
            if !matches!(
                join.join_type,
                crate::plans::JoinType::Inner | crate::plans::JoinType::Cross
            ) {
                return;
            }

            let mut left_signature = None;
            let mut left_path = path.clone();
            left_path.push(0);
            'left: for (signature, entries) in signature_to_exprs.iter() {
                for (entry_path, _) in entries {
                    if entry_path == &left_path {
                        left_signature = Some(signature.clone());
                        break 'left;
                    }
                }
            }

            let mut right_signature = None;
            let mut right_path = path.clone();
            right_path.push(1);
            'right: for (signature, entries) in signature_to_exprs.iter() {
                for (entry_path, _) in entries {
                    if entry_path == &right_path {
                        right_signature = Some(signature.clone());
                        break 'right;
                    }
                }
            }

            let (Some(left_signature), Some(right_signature)) =
                (left_signature, right_signature)
            else {
                return;
            };

            let mut tables = left_signature.tables.clone();
            tables.extend(right_signature.tables.iter().copied());
            signature_to_exprs
                .entry(SExprSignature { tables })
                .or_default()
                .push((path.clone(), expr.clone()));
        }
        _ => {}
    }
}
