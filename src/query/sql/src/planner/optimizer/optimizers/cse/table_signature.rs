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

use databend_common_expression::ColumnId;
use databend_common_expression::FunctionKind;
use databend_common_functions::BUILTIN_FUNCTIONS;

use crate::ColumnBindingBuilder;
use crate::ColumnEntry;
use crate::IndexType;
use crate::ScalarExpr;
use crate::Symbol;
use crate::Visibility;
use crate::optimizer::ir::SExpr;
use crate::planner::metadata::Metadata;
use crate::plans::Aggregate;
use crate::plans::FunctionCall;
use crate::plans::Join;
use crate::plans::JoinType;
use crate::plans::RelOperator;
use crate::plans::ScalarItem;
use crate::plans::Scan;
use crate::plans::Visitor;
use crate::plans::VisitorMut;
use crate::plans::walk_expr_mut;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct TableSignature {
    pub scans: Vec<ScanSignature>,
    pub aggregate: Option<AggregateSignature>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ScanSignature {
    pub table_id: IndexType,
    pub columns: Vec<ColumnSignature>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ColumnSignature {
    Base {
        column_id: ColumnId,
        path_indices: Option<Vec<usize>>,
        virtual_expr: Option<String>,
    },
    Virtual {
        source_column_id: ColumnId,
        column_id: ColumnId,
        key_paths: String,
        is_try: bool,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct AggregateSignature {
    pub aggregate: Aggregate,
    pub input_items: Vec<ScalarItem>,
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
) -> Option<Vec<ScanSignature>> {
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
            let scan = scan_signature(scan, metadata)?;
            let tables = vec![scan];
            signature_to_exprs
                .entry(TableSignature {
                    scans: tables.clone(),
                    aggregate: None,
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
                    scans: tables.clone(),
                    aggregate: None,
                })
                .or_default()
                .push((path.clone(), expr.clone()));
            Some(tables)
        }
        RelOperator::Aggregate(aggregate) if child_tables.len() == 1 => {
            let tables = child_tables[0]
                .clone()
                .or_else(|| aggregate_input_tables(expr.child(0).ok()?, metadata))?;
            if let Some(aggregate_signature) = aggregate_signature(aggregate, expr.child(0).ok()?) {
                signature_to_exprs
                    .entry(TableSignature {
                        scans: tables.clone(),
                        aggregate: Some(aggregate_signature),
                    })
                    .or_default()
                    .push((path.clone(), expr.clone()));
            }
            None
        }
        _ => None,
    }
}

fn aggregate_input_tables(expr: &SExpr, metadata: &Metadata) -> Option<Vec<ScanSignature>> {
    match expr.plan() {
        RelOperator::EvalScalar(_) if expr.arity() == 1 => {
            aggregate_input_tables_without_eval_scalar(expr.child(0).ok()?, metadata)
        }
        _ => aggregate_input_tables_without_eval_scalar(expr, metadata),
    }
}

fn aggregate_input_tables_without_eval_scalar(
    expr: &SExpr,
    metadata: &Metadata,
) -> Option<Vec<ScanSignature>> {
    match expr.plan() {
        RelOperator::Scan(scan) => Some(vec![scan_signature(scan, metadata)?]),
        RelOperator::Join(join) if is_supported_cross_join(join) && expr.arity() == 2 => {
            let mut tables =
                aggregate_input_tables_without_eval_scalar(expr.child(0).ok()?, metadata)?;
            tables.extend(aggregate_input_tables_without_eval_scalar(
                expr.child(1).ok()?,
                metadata,
            )?);
            Some(tables)
        }
        _ => None,
    }
}

fn aggregate_signature(aggregate: &Aggregate, input: &SExpr) -> Option<AggregateSignature> {
    if aggregate.rank_limit.is_some() || aggregate.grouping_sets.is_some() {
        return None;
    }

    let input_columns = input
        .derive_relational_prop()
        .ok()?
        .output_columns
        .iter()
        .copied()
        .enumerate()
        .map(|(position, column)| (column, Symbol::new(position)))
        .collect::<HashMap<_, _>>();

    let mut aggregate = aggregate.clone();
    aggregate.group_items = normalize_scalar_items(&aggregate.group_items, &input_columns)?;
    aggregate.aggregate_functions =
        normalize_scalar_items(&aggregate.aggregate_functions, &input_columns)?;
    let input_items = aggregate_input_items(input)?;

    if !scalar_items_are_deterministic(&aggregate.group_items)
        || !scalar_items_are_deterministic(&aggregate.aggregate_functions)
        || !scalar_items_are_deterministic(&input_items)
    {
        return None;
    }

    Some(AggregateSignature {
        aggregate,
        input_items,
    })
}

fn aggregate_input_items(input: &SExpr) -> Option<Vec<ScalarItem>> {
    let RelOperator::EvalScalar(eval_scalar) = input.plan() else {
        return Some(vec![]);
    };

    let input_columns = input
        .derive_relational_prop()
        .ok()?
        .output_columns
        .iter()
        .copied()
        .enumerate()
        .map(|(position, column)| (column, Symbol::new(position)))
        .collect::<HashMap<_, _>>();
    let child_columns = input
        .child(0)
        .ok()?
        .derive_relational_prop()
        .ok()?
        .output_columns
        .iter()
        .copied()
        .enumerate()
        .map(|(position, column)| (column, Symbol::new(position)))
        .collect::<HashMap<_, _>>();

    eval_scalar
        .items
        .iter()
        .map(|item| {
            Some(ScalarItem {
                scalar: normalize_scalar_expr(&item.scalar, &child_columns)?,
                index: *input_columns.get(&item.index)?,
            })
        })
        .collect()
}

fn scalar_items_are_deterministic(items: &[ScalarItem]) -> bool {
    items
        .iter()
        .all(|item| scalar_expr_is_deterministic(&item.scalar))
}

fn scalar_expr_is_deterministic(scalar: &ScalarExpr) -> bool {
    let mut visitor = DeterministicVisitor {
        deterministic: true,
    };
    visitor.visit(scalar).is_ok() && visitor.deterministic
}

struct DeterministicVisitor {
    deterministic: bool,
}

impl<'a> Visitor<'a> for DeterministicVisitor {
    fn visit_function_call(
        &mut self,
        func: &'a FunctionCall,
    ) -> databend_common_exception::Result<()> {
        if BUILTIN_FUNCTIONS
            .get_property(&func.func_name)
            .map(|property| property.non_deterministic || property.kind == FunctionKind::SRF)
            .unwrap_or(true)
        {
            self.deterministic = false;
            return Ok(());
        }

        for expr in &func.arguments {
            self.visit(expr)?;
        }
        Ok(())
    }
}

fn normalize_scalar_items(
    items: &[ScalarItem],
    input_columns: &HashMap<Symbol, Symbol>,
) -> Option<Vec<ScalarItem>> {
    items
        .iter()
        .enumerate()
        .map(|(position, item)| {
            Some(ScalarItem {
                scalar: normalize_scalar_expr(&item.scalar, input_columns)?,
                index: Symbol::new(position),
            })
        })
        .collect()
}

fn normalize_scalar_expr(
    scalar: &ScalarExpr,
    input_columns: &HashMap<Symbol, Symbol>,
) -> Option<ScalarExpr> {
    let mut scalar = scalar.clone();
    let mut visitor = NormalizeColumnVisitor { input_columns };
    visitor.visit(&mut scalar).ok()?;
    Some(scalar)
}

struct NormalizeColumnVisitor<'a> {
    input_columns: &'a HashMap<Symbol, Symbol>,
}

impl VisitorMut<'_> for NormalizeColumnVisitor<'_> {
    fn visit(&mut self, expr: &mut ScalarExpr) -> databend_common_exception::Result<()> {
        walk_expr_mut(self, expr)
    }

    fn visit_bound_column_ref(
        &mut self,
        col: &mut crate::plans::BoundColumnRef,
    ) -> databend_common_exception::Result<()> {
        let Some(normalized) = self.input_columns.get(&col.column.index) else {
            return Err(databend_common_exception::ErrorCode::Internal(
                "aggregate CSE column is not produced by input",
            ));
        };
        col.column = ColumnBindingBuilder::new(
            normalized.to_string(),
            *normalized,
            col.column.data_type.clone(),
            Visibility::Visible,
        )
        .build();
        Ok(())
    }

    fn visit_aggregate_function(
        &mut self,
        aggregate: &mut crate::plans::AggregateFunction,
    ) -> databend_common_exception::Result<()> {
        aggregate.display_name.clear();
        for expr in aggregate.exprs_mut() {
            self.visit(expr)?;
        }
        Ok(())
    }
}

fn scan_signature(scan: &Scan, metadata: &Metadata) -> Option<ScanSignature> {
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

    let mut columns = scan
        .columns
        .iter()
        .map(|column_index| column_signature(metadata.column(*column_index)))
        .collect::<Option<Vec<_>>>()?;
    columns.sort();

    Some(ScanSignature {
        table_id: table.get_id() as IndexType,
        columns,
    })
}

fn column_signature(column: &ColumnEntry) -> Option<ColumnSignature> {
    match column {
        ColumnEntry::BaseTableColumn(base) => Some(ColumnSignature::Base {
            column_id: base.column_id,
            path_indices: base.path_indices.clone(),
            virtual_expr: base.virtual_expr.clone(),
        }),
        ColumnEntry::VirtualColumn(virtual_column) => Some(ColumnSignature::Virtual {
            source_column_id: virtual_column.source_column_id,
            column_id: virtual_column.column_id,
            key_paths: format!("{:?}", virtual_column.key_paths),
            is_try: virtual_column.is_try,
        }),
        ColumnEntry::InternalColumn(_) | ColumnEntry::DerivedColumn(_) => None,
    }
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
