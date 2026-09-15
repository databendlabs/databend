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
use std::sync::Arc;

use databend_common_ast::parser::parse_cluster_key_exprs;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::Expr;
use databend_common_expression::types::DataType;
use databend_storages_common_table_meta::table::ClusterType;
use databend_storages_common_table_meta::table::cluster_type_from_options;

use crate::BaseTableColumn;
use crate::ColumnEntry;
use crate::MetadataRef;
use crate::ScalarExpr;
use crate::Symbol;
use crate::optimizer::ir::SExpr;
use crate::parse_ast_exprs;
use crate::plans::JoinType;
use crate::plans::RelOperator;
use crate::plans::Scan;

/// Locality evidence used only by join ordering. It is not a row-count estimate
/// or a guarantee of physical output ordering.
#[derive(Clone, Default)]
pub(super) struct ClusterKeyState {
    pub keys: Vec<Expr<Symbol>>,
    pub filter_keys: Vec<Expr<Symbol>>,
}

pub(super) struct ClusterKeyCostModel<'a> {
    metadata: &'a MetadataRef,
    table_ctx: Arc<dyn TableContext>,
    factor: f64,
}

impl<'a> ClusterKeyCostModel<'a> {
    pub fn new(metadata: &'a MetadataRef, table_ctx: Arc<dyn TableContext>, percent: u64) -> Self {
        Self {
            metadata,
            table_ctx,
            factor: percent as f64 / 100.0,
        }
    }

    pub fn enabled(&self) -> bool {
        self.factor > 0.0
    }

    #[recursive::recursive]
    pub fn collect(&self, expr: &SExpr) -> Result<ClusterKeyState> {
        if !self.enabled() {
            return Ok(ClusterKeyState::default());
        }
        match expr.plan() {
            RelOperator::Scan(scan) => self.scan_keys(scan),
            RelOperator::Filter(filter) => {
                let mut state = self.collect(expr.unary_child())?;
                if !state.keys.is_empty() {
                    collect_filter_keys(&filter.predicates, &mut state.filter_keys)?;
                }
                Ok(state)
            }
            RelOperator::EvalScalar(_) | RelOperator::Limit(_) => self.collect(expr.unary_child()),
            RelOperator::Join(join) if join.join_type == JoinType::Inner => {
                self.collect(expr.child(0)?)
            }
            // Aggregation, sorting, exchanges, unions and CTE references do not
            // establish the probe locality assumed by this heuristic.
            _ => Ok(ClusterKeyState::default()),
        }
    }

    fn scan_keys(&self, scan: &Scan) -> Result<ClusterKeyState> {
        if scan.change_type.is_some() {
            return Ok(ClusterKeyState::default());
        }
        let (table, columns) = {
            let metadata = self.metadata.read();
            let table = metadata.table(scan.table_index).table();
            let columns = metadata
                .columns_by_table_index(scan.table_index)
                .filter_map(|entry| {
                    if let ColumnEntry::BaseTableColumn(BaseTableColumn {
                        column_id,
                        column_index,
                        virtual_expr: None,
                        ..
                    }) = entry
                    {
                        Some((*column_id, *column_index))
                    } else {
                        None
                    }
                })
                .collect::<HashMap<_, _>>();
            (table, columns)
        };
        let Some((_, key)) = table.cluster_key_meta() else {
            return Ok(ClusterKeyState::default());
        };
        if cluster_type_from_options(table.options()) != ClusterType::Linear {
            return Ok(ClusterKeyState::default());
        }
        // Match declared logical keys. Storage binding normalizes string keys
        // into substr expressions, which would hide ordinary equality matches.
        let keys = parse_ast_exprs(
            self.table_ctx.clone(),
            table.clone(),
            parse_cluster_key_exprs(&key)?,
        )?;
        if keys
            .iter()
            .any(|key| matches!(key.data_type().remove_nullable(), DataType::Vector(_)))
        {
            return Ok(ClusterKeyState::default());
        }
        let schema = table.schema();
        let mut state = ClusterKeyState::default();
        for key in keys {
            let refs = key.column_refs();
            if !refs.keys().all(|binding| {
                schema
                    .column_id_of(&binding.column_name)
                    .is_ok_and(|id| columns.contains_key(&id))
            }) {
                // A missing leading key prevents use of every following key.
                break;
            }
            let key = key.project_column_ref(|binding| {
                Ok(columns[&schema.column_id_of(&binding.column_name)?])
            })?;
            state.keys.push(normalize_key(key));
        }
        if !state.keys.is_empty() {
            collect_filter_keys(
                scan.push_down_predicates
                    .iter()
                    .flatten()
                    .chain(scan.prewhere.iter().flat_map(|p| &p.predicates)),
                &mut state.filter_keys,
            )?;
        }
        Ok(state)
    }

    pub fn factor(&self, keys: &[Expr<Symbol>], candidates: &[Expr<Symbol>]) -> f64 {
        if !self.enabled() {
            return 1.0;
        }
        prefix_factor(self.factor, keys, candidates)
    }
}

/// Column display names contain binder-local indexes and aliases. Equality of
/// clustered expressions must depend on query symbols, not those display names.
pub(super) fn normalize_key(mut expr: Expr<Symbol>) -> Expr<Symbol> {
    #[recursive::recursive]
    fn clear_names(expr: &mut Expr<Symbol>) {
        match expr {
            Expr::ColumnRef(column) => column.display_name.clear(),
            Expr::Cast(cast) => clear_names(&mut cast.expr),
            Expr::FunctionCall(call) => call.args.iter_mut().for_each(clear_names),
            Expr::LambdaFunctionCall(call) => call.args.iter_mut().for_each(clear_names),
            Expr::Constant(_) => {}
        }
    }
    clear_names(&mut expr);
    expr
}

fn collect_filter_keys<'a>(
    predicates: impl IntoIterator<Item = &'a ScalarExpr>,
    keys: &mut Vec<Expr<Symbol>>,
) -> Result<()> {
    let mut stack = predicates.into_iter().collect::<Vec<_>>();
    while let Some(predicate) = stack.pop() {
        let ScalarExpr::FunctionCall(call) = predicate else {
            continue;
        };
        match call.func_name.as_str() {
            "and" | "and_filters" => stack.extend(&call.arguments),
            "eq" if call.arguments.len() == 2 => {
                let left = &call.arguments[0];
                let right = &call.arguments[1];
                // Earlier folding turns constant expressions into literals.
                // Column-free expressions such as rand() are not constant filters.
                match (
                    matches!(
                        left,
                        ScalarExpr::ConstantExpr(_) | ScalarExpr::TypedConstantExpr(_, _)
                    ),
                    matches!(
                        right,
                        ScalarExpr::ConstantExpr(_) | ScalarExpr::TypedConstantExpr(_, _)
                    ),
                ) {
                    (false, true) if !left.used_columns().is_empty() => {
                        keys.push(normalize_key(left.as_symbol_expr()?))
                    }
                    (true, false) if !right.used_columns().is_empty() => {
                        keys.push(normalize_key(right.as_symbol_expr()?))
                    }
                    _ => {}
                }
            }
            _ => {}
        }
    }
    Ok(())
}

fn prefix_factor(factor: f64, keys: &[Expr<Symbol>], candidates: &[Expr<Symbol>]) -> f64 {
    let mut used = vec![false; candidates.len()];
    let mut result = 1.0;
    for key in keys {
        let Some(index) = candidates.iter().enumerate().position(|(i, candidate)| {
            !used[i]
                && match (key, candidate) {
                    (Expr::ColumnRef(left), Expr::ColumnRef(right)) => left.id == right.id,
                    _ => key == candidate,
                }
        }) else {
            break;
        };
        used[index] = true;
        result *= factor;
    }
    result
}

#[cfg(test)]
mod tests {
    use databend_common_expression::ColumnRef;
    use databend_common_expression::types::DataType;
    use databend_common_expression::types::NumberDataType;

    use super::*;

    fn key(id: usize) -> Expr<Symbol> {
        Expr::ColumnRef(ColumnRef {
            span: None,
            id: Symbol::new(id),
            data_type: DataType::Number(NumberDataType::Int64),
            display_name: format!("k{id}"),
        })
    }

    #[test]
    fn expression_keys_ignore_display_names_but_keep_symbol_identity() {
        let cast = |expr| {
            Expr::Cast(databend_common_expression::Cast {
                span: None,
                is_try: false,
                expr: Box::new(expr),
                dest_type: DataType::String,
            })
        };
        let mut renamed = key(0);
        let Expr::ColumnRef(column) = &mut renamed else {
            unreachable!()
        };
        column.display_name = "another_alias.k0 (#123)".to_string();
        assert_eq!(normalize_key(cast(key(0))), normalize_key(cast(renamed)));
        assert_ne!(normalize_key(cast(key(0))), normalize_key(cast(key(1))));
    }

    #[test]
    fn prefix_requires_leading_keys_and_distinct_candidates() {
        let keys = vec![key(0), key(1)];
        assert_eq!(prefix_factor(0.5, &keys, &[key(1)]), 1.0);
        assert_eq!(prefix_factor(0.5, &keys, &[key(0)]), 0.5);
        assert_eq!(prefix_factor(0.5, &keys, &[key(1), key(0)]), 0.25);
        assert_eq!(prefix_factor(0.5, &keys, &[key(0), key(0)]), 0.5);
        assert_eq!(prefix_factor(0.5, &[key(0), key(0)], &[key(0)]), 0.5);
        assert_eq!(prefix_factor(0.5, &keys, &[key(2)]), 1.0);
        assert_eq!(prefix_factor(1.0, &keys, &keys), 1.0);
    }
}
