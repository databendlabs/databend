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

use std::sync::Arc;

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_storages_common_table_meta::table::ClusterType;
use databend_storages_common_table_meta::table::cluster_type_from_options;

use crate::BaseTableColumn;
use crate::ColumnEntry;
use crate::MetadataRef;
use crate::NameResolutionContext;
use crate::ScalarExpr;
use crate::TypeChecker;
use crate::Visibility;
use crate::binder::BindContext;
use crate::binder::ColumnBindingBuilder;
use crate::optimizer::ir::SExpr;
use crate::optimizer::ir::SExprVisitor;
use crate::optimizer::ir::VisitAction;
use crate::plans::JoinType;
use crate::plans::RelOperator;
use crate::plans::Scan;

/// Locality evidence used only by join ordering. It is not a row-count estimate
/// or a guarantee of physical output ordering.
#[derive(Clone, Default)]
pub(super) struct ClusterKeyState {
    pub keys: Vec<ScalarExpr>,
    pub filter_keys: Vec<ScalarExpr>,
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

    pub fn collect(&self, expr: &SExpr) -> Result<ClusterKeyState> {
        if !self.enabled() {
            return Ok(ClusterKeyState::default());
        }

        struct ClusterKeyCollector<'a, 'b> {
            model: &'a ClusterKeyCostModel<'b>,
            state: ClusterKeyState,
        }

        impl SExprVisitor for ClusterKeyCollector<'_, '_> {
            fn visit(&mut self, expr: &SExpr) -> Result<VisitAction> {
                match expr.plan() {
                    RelOperator::Scan(scan) => {
                        self.state = self.model.scan_keys(scan)?;
                        Ok(VisitAction::Stop)
                    }
                    RelOperator::Filter(_) | RelOperator::EvalScalar(_) | RelOperator::Limit(_) => {
                        Ok(VisitAction::Continue)
                    }
                    RelOperator::Join(join) if join.join_type == JoinType::Inner => {
                        expr.left_child().accept(self)?;
                        Ok(VisitAction::Stop)
                    }
                    // Joins, aggregation, sorting, exchanges, unions and CTE references do not
                    // establish the probe locality assumed by this heuristic.
                    _ => Ok(VisitAction::Stop),
                }
            }

            fn post_visit(&mut self, expr: &SExpr) -> Result<VisitAction> {
                if !self.state.keys.is_empty()
                    && let RelOperator::Filter(filter) = expr.plan()
                {
                    collect_filter_keys(&filter.predicates, &mut self.state.filter_keys);
                }
                Ok(VisitAction::Continue)
            }
        }

        let mut collector = ClusterKeyCollector {
            model: self,
            state: ClusterKeyState::default(),
        };
        expr.accept(&mut collector)?;
        Ok(collector.state)
    }

    fn scan_keys(&self, scan: &Scan) -> Result<ClusterKeyState> {
        if scan.change_type.is_some() {
            return Ok(ClusterKeyState::default());
        }
        let table = self.metadata.read().table(scan.table_index).table();
        if cluster_type_from_options(table.options()) != ClusterType::Linear {
            return Ok(ClusterKeyState::default());
        }
        let Some(keys) = table.resolve_cluster_keys() else {
            return Ok(ClusterKeyState::default());
        };

        // Bind persisted keys directly to this scan's query symbols and table
        // instance. A separate scope keeps other tables and self-join aliases out.
        let mut bind_context = BindContext::new();
        {
            let metadata = self.metadata.read();
            for entry in metadata.columns_by_table_index(scan.table_index) {
                if let ColumnEntry::BaseTableColumn(BaseTableColumn {
                    column_index,
                    column_name,
                    data_type,
                    path_indices,
                    virtual_expr,
                    ..
                }) = entry
                {
                    let visibility = if path_indices.is_some() {
                        Visibility::InVisible
                    } else {
                        Visibility::Visible
                    };
                    bind_context.add_column_binding(
                        ColumnBindingBuilder::new(
                            column_name.clone(),
                            *column_index,
                            Box::new(data_type.into()),
                            visibility,
                        )
                        .database_name(Some(
                            metadata.table(scan.table_index).database().to_string(),
                        ))
                        .table_name(Some(table.name().to_string()))
                        .table_index(Some(scan.table_index))
                        .virtual_expr(virtual_expr.clone())
                        .build(),
                    );
                }
            }
        }
        let settings = self.table_ctx.get_settings();
        let name_resolution_ctx = NameResolutionContext::try_from(settings.as_ref())?;
        let mut type_checker = TypeChecker::try_create(
            &mut bind_context,
            self.table_ctx.clone(),
            &name_resolution_ctx,
            self.metadata.clone(),
            &[],
            false,
        )?;
        let mut state = ClusterKeyState::default();
        for ast in keys {
            let (scalar, _) = *type_checker.resolve(&ast)?;
            if matches!(scalar.data_type().remove_nullable(), DataType::Vector(_)) {
                return Ok(ClusterKeyState::default());
            }
            state.keys.push(scalar);
        }
        if !state.keys.is_empty() {
            collect_filter_keys(
                scan.push_down_predicates
                    .iter()
                    .flatten()
                    .chain(scan.prewhere.iter().flat_map(|p| &p.predicates)),
                &mut state.filter_keys,
            );
        }
        Ok(state)
    }

    pub fn factor(&self, keys: &[ScalarExpr], candidates: &[ScalarExpr]) -> f64 {
        if !self.enabled() {
            return 1.0;
        }
        prefix_factor(self.factor, keys, candidates)
    }
}

fn collect_filter_keys<'a>(
    predicates: impl IntoIterator<Item = &'a ScalarExpr>,
    keys: &mut Vec<ScalarExpr>,
) {
    let mut stack = predicates.into_iter().collect::<Vec<_>>();
    while let Some(predicate) = stack.pop() {
        let ScalarExpr::FunctionCall(call) = predicate else {
            continue;
        };
        match call.func_name.as_str() {
            "and" | "and_filters" => stack.extend(&call.arguments),
            "eq" if let [left, right] = call.arguments.as_slice() => {
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
                    (false, true) if !left.used_columns().is_empty() => keys.push(left.clone()),
                    (true, false) if !right.used_columns().is_empty() => keys.push(right.clone()),
                    _ => {}
                }
            }
            _ => {}
        }
    }
}

fn prefix_factor(factor: f64, keys: &[ScalarExpr], candidates: &[ScalarExpr]) -> f64 {
    let mut used = vec![false; candidates.len()];
    let mut matched = 0;
    for key in keys {
        let Some(index) = candidates
            .iter()
            .enumerate()
            .position(|(i, candidate)| !used[i] && key == candidate)
        else {
            break;
        };
        used[index] = true;
        matched += 1;
    }
    if matched == 0 {
        1.0
    } else if matched == keys.len() {
        factor
    } else {
        // A partial prefix receives half the benefit of a complete match.
        1.0 - (1.0 - factor) / 2.0
    }
}

#[cfg(test)]
mod tests {
    use databend_common_expression::types::DataType;
    use databend_common_expression::types::NumberDataType;

    use super::*;
    use crate::Symbol;
    use crate::plans::BoundColumnRef;
    use crate::plans::CastExpr;

    fn key(id: usize) -> ScalarExpr {
        BoundColumnRef {
            span: None,
            column: ColumnBindingBuilder::new(
                format!("k{id}"),
                Symbol::new(id),
                Box::new(DataType::Number(NumberDataType::Int64)),
                Visibility::Visible,
            )
            .table_index(Some(0))
            .build(),
        }
        .into()
    }

    #[test]
    fn expression_keys_ignore_display_names_but_keep_symbol_identity() {
        let cast = |expr| {
            ScalarExpr::CastExpr(CastExpr {
                span: None,
                is_try: false,
                argument: Box::new(expr),
                target_type: Box::new(DataType::String),
            })
        };
        let mut renamed = key(0);
        let ScalarExpr::BoundColumnRef(column) = &mut renamed else {
            unreachable!()
        };
        column.column.column_name = "another_alias".to_string();
        column.column.table_name = Some("renamed".to_string());
        assert_eq!(prefix_factor(0.5, &[cast(key(0))], &[cast(renamed)]), 0.5);
        assert_eq!(prefix_factor(0.5, &[cast(key(0))], &[cast(key(1))]), 1.0);
        let mut other_table = key(0);
        let ScalarExpr::BoundColumnRef(column) = &mut other_table else {
            unreachable!()
        };
        column.column.table_index = Some(1);
        assert_eq!(
            prefix_factor(0.5, &[cast(key(0))], &[cast(other_table)]),
            1.0
        );
    }

    #[test]
    fn prefix_requires_leading_keys_and_distinct_candidates() {
        let keys = vec![key(0), key(1)];
        assert_eq!(prefix_factor(0.5, &keys, &[key(1)]), 1.0);
        assert_eq!(prefix_factor(0.5, &keys, &[key(0)]), 0.75);
        assert_eq!(prefix_factor(0.5, &keys, &[key(1), key(0)]), 0.5);
        assert_eq!(prefix_factor(0.5, &keys, &[key(0), key(0)]), 0.75);
        assert_eq!(prefix_factor(0.5, &[key(0), key(0)], &[key(0)]), 0.75);
        assert_eq!(prefix_factor(0.5, &keys, &[key(2)]), 1.0);
        assert_eq!(prefix_factor(1.0, &keys, &keys), 1.0);
    }
}
