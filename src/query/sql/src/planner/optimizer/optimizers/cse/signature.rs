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
use crate::plans::AggregateMode;
use crate::plans::RelOperator;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SExprSignature {
    pub tables: BTreeSet<IndexType>,
    pub has_aggregate: bool,
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
) -> Option<SExprSignature> {
    let mut child_signatures = Vec::with_capacity(expr.children.len());
    for (child_index, child) in expr.children().enumerate() {
        path.push(child_index);
        let child_signature =
            compute_s_expr_signature_rec(child, path, metadata, signature_to_exprs);
        path.pop();
        child_signatures.push(child_signature);
    }

    if child_signatures
        .iter()
        .any(|sig| sig.as_ref().is_none_or(|sig| sig.has_aggregate))
    {
        return None;
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
                return None;
            }

            let table_entry = metadata.table(scan.table_index);
            let table = table_entry.table();
            if table.engine() != "FUSE" {
                return None;
            }

            let mut tables = BTreeSet::new();
            tables.insert(table.get_id() as IndexType);
            let signature = SExprSignature {
                tables,
                has_aggregate: false,
            };
            signature_to_exprs
                .entry(signature.clone())
                .or_default()
                .push((path.clone(), expr.clone()));
            Some(signature)
        }
        RelOperator::Aggregate(aggregate) => {
            let child_signature = child_signatures.first()?.as_ref()?;

            let has_aggregate = match aggregate.mode {
                AggregateMode::Partial => false,
                AggregateMode::Final | AggregateMode::Initial => true,
            };

            let signature = SExprSignature {
                tables: child_signature.tables.clone(),
                has_aggregate,
            };
            if !matches!(aggregate.mode, AggregateMode::Partial) {
                signature_to_exprs
                    .entry(signature.clone())
                    .or_default()
                    .push((path.clone(), expr.clone()));
            }
            Some(signature)
        }
        RelOperator::Join(join) => {
            if join.from_correlated_subquery || join.is_lateral {
                return None;
            }
            if !matches!(
                join.join_type,
                crate::plans::JoinType::Inner | crate::plans::JoinType::Cross
            ) {
                return None;
            }

            let (Some(left_signature), Some(right_signature)) =
                (&child_signatures[0], &child_signatures[1])
            else {
                return None;
            };

            let mut tables = left_signature.tables.clone();
            tables.extend(right_signature.tables.iter().copied());
            let signature = SExprSignature {
                tables,
                has_aggregate: false,
            };
            signature_to_exprs
                .entry(signature.clone())
                .or_default()
                .push((path.clone(), expr.clone()));
            Some(signature)
        }
        _ => None,
    }
}
