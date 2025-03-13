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
use std::collections::HashSet;
use std::sync::Arc;

use databend_common_ast::ast::Expr;
use databend_common_ast::ast::SelectStmt;
use databend_common_ast::ast::SelectTarget;
use databend_common_ast::ast::SetExpr;
use databend_common_ast::ast::SetOperator;
use databend_common_ast::Span;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::type_check::common_super_type;
use databend_common_expression::types::DataType;
use databend_common_expression::ROW_ID_COLUMN_ID;
use databend_common_expression::ROW_ID_COL_NAME;
use databend_common_functions::BUILTIN_FUNCTIONS;

use super::sort::OrderItem;
use super::Finder;
use crate::binder::bind_table_reference::JoinConditions;
use crate::binder::scalar_common::split_conjunctions;
use crate::binder::ColumnBindingBuilder;
use crate::binder::ExprContext;
use crate::binder::INTERNAL_COLUMN_FACTORY;
use crate::optimizer::SExpr;
use crate::planner::binder::scalar::ScalarBinder;
use crate::planner::binder::BindContext;
use crate::planner::binder::Binder;
use crate::plans::BoundColumnRef;
use crate::plans::CastExpr;
use crate::plans::Filter;
use crate::plans::JoinType;
use crate::plans::ScalarExpr;
use crate::plans::ScalarItem;
use crate::plans::UnionAll;
use crate::plans::Visitor as _;
use crate::ColumnEntry;
use crate::IndexType;
use crate::Visibility;

// A normalized IR for `SELECT` clause.
#[derive(Debug, Default)]
pub struct SelectList<'a> {
    pub items: Vec<SelectItem<'a>>,
}

#[derive(Debug)]
pub struct SelectItem<'a> {
    pub select_target: &'a SelectTarget,
    pub scalar: ScalarExpr,
    pub alias: String,
}

impl Binder {
    pub fn bind_where(
        &mut self,
        bind_context: &mut BindContext,
        aliases: &[(String, ScalarExpr)],
        expr: &Expr,
        child: SExpr,
    ) -> Result<(SExpr, ScalarExpr)> {
        let last_expr_context = bind_context.expr_context.clone();
        bind_context.set_expr_context(ExprContext::WhereClause);

        let mut scalar_binder = ScalarBinder::new(
            bind_context,
            self.ctx.clone(),
            &self.name_resolution_ctx,
            self.metadata.clone(),
            aliases,
        );
        let (scalar, _) = scalar_binder.bind(expr)?;
        let f = |scalar: &ScalarExpr| {
            matches!(
                scalar,
                ScalarExpr::AggregateFunction(_) | ScalarExpr::WindowFunction(_)
            )
        };

        let mut finder = Finder::new(&f);
        finder.visit(&scalar)?;
        if !finder.scalars().is_empty() {
            return Err(ErrorCode::SemanticError(
                "Where clause can't contain aggregate or window functions".to_string(),
            )
            .set_span(scalar.span()));
        }

        let filter_plan = Filter {
            predicates: split_conjunctions(&scalar),
        };
        let new_expr = SExpr::create_unary(Arc::new(filter_plan.into()), Arc::new(child));
        bind_context.set_expr_context(last_expr_context);
        Ok((new_expr, scalar))
    }

    #[recursive::recursive]
    pub(super) fn bind_set_operator(
        &mut self,
        bind_context: &mut BindContext,
        left: &SetExpr,
        right: &SetExpr,
        op: &SetOperator,
        all: &bool,
        cte_name: Option<String>,
    ) -> Result<(SExpr, BindContext)> {
        let (left_expr, left_bind_context) =
            self.bind_set_expr(bind_context, left, &[], None, cte_name.clone())?;
        if let Some(cte_name) = cte_name.as_ref() {
            if !all {
                return Err(ErrorCode::Internal(
                    "Currently, recursive cte only support union all".to_string(),
                ));
            }
            // Add recursive cte's columns to cte info
            let mut_cte_info = bind_context.cte_context.cte_map.get_mut(cte_name).unwrap();
            // The recursive cte may be used by multiple times in main query, so clear cte_info's columns
            mut_cte_info.columns.clear();
            for column in left_bind_context.columns.iter() {
                let col = ColumnBindingBuilder::new(
                    column.column_name.clone(),
                    column.index,
                    column.data_type.clone(),
                    Visibility::Visible,
                )
                .table_name(Some(cte_name.clone()))
                .build();
                mut_cte_info.columns.push(col);
            }
        }
        // Merge cte info from left context to `bind_context`
        bind_context
            .cte_context
            .merge(left_bind_context.cte_context.clone());
        let (right_expr, right_bind_context) =
            self.bind_set_expr(bind_context, right, &[], None, None)?;

        if left_bind_context.columns.len() != right_bind_context.columns.len() {
            return Err(ErrorCode::SemanticError(
                "SetOperation must have the same number of columns",
            ));
        }

        match (op, all) {
            (SetOperator::Intersect, false) => {
                // Transfer Intersect to Semi join
                self.bind_intersect(
                    left.span(),
                    right.span(),
                    left_bind_context,
                    right_bind_context,
                    left_expr,
                    right_expr,
                )
            }
            (SetOperator::Except, false) => {
                // Transfer Except to Anti join
                self.bind_except(
                    left.span(),
                    right.span(),
                    left_bind_context,
                    right_bind_context,
                    left_expr,
                    right_expr,
                )
            }
            (SetOperator::Union, true) => self.bind_union(
                left.span(),
                right.span(),
                left_bind_context,
                right_bind_context,
                left_expr,
                right_expr,
                false,
                cte_name,
            ),
            (SetOperator::Union, false) => self.bind_union(
                left.span(),
                right.span(),
                left_bind_context,
                right_bind_context,
                left_expr,
                right_expr,
                true,
                cte_name,
            ),
            _ => Err(ErrorCode::Unimplemented(
                "Unsupported query type, currently, databend only support intersect distinct and except distinct",
            )),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn bind_union(
        &mut self,
        left_span: Span,
        right_span: Span,
        left_context: BindContext,
        right_context: BindContext,
        left_expr: SExpr,
        right_expr: SExpr,
        distinct: bool,
        cte_name: Option<String>,
    ) -> Result<(SExpr, BindContext)> {
        let mut coercion_types = Vec::with_capacity(left_context.columns.len());
        let mut cte_scan_names = Vec::new();
        if cte_name.is_some() {
            self.count_r_cte_scan(&right_expr, &mut cte_scan_names, &mut coercion_types)?;
            if cte_scan_names.is_empty() {
                return Err(ErrorCode::SemanticError(
                    "Recursive cte should be used in recursive cte".to_string(),
                ));
            }
        } else {
            for (left_col, right_col) in left_context
                .columns
                .iter()
                .zip(right_context.columns.iter())
            {
                if left_col.data_type != right_col.data_type {
                    if let Some(data_type) = common_super_type(
                        *left_col.data_type.clone(),
                        *right_col.data_type.clone(),
                        &BUILTIN_FUNCTIONS.default_cast_rules,
                    ) {
                        coercion_types.push(data_type);
                    } else {
                        return Err(ErrorCode::SemanticError(format!(
                            "SetOperation's types cannot be matched, left column {:?}, type: {:?}, right column {:?}, type: {:?}",
                            left_col.column_name,
                            left_col.data_type,
                            right_col.column_name,
                            right_col.data_type
                        )));
                    }
                } else {
                    coercion_types.push(*left_col.data_type.clone());
                }
            }
        }

        let (mut new_bind_context, left_outputs, right_outputs) = self.coercion_union_type(
            left_span,
            right_span,
            left_context,
            right_context.clone(),
            coercion_types,
        )?;

        if let Some(cte_name) = &cte_name {
            for (col, cte_col) in new_bind_context.columns.iter_mut().zip(
                new_bind_context
                    .cte_context
                    .cte_map
                    .get(cte_name)
                    .unwrap()
                    .columns
                    .iter(),
            ) {
                col.table_name = cte_col.table_name.clone();
                col.column_name = cte_col.column_name.clone();
            }
        }

        let output_indexes = new_bind_context.columns.iter().map(|x| x.index).collect();

        let union_plan = UnionAll {
            left_outputs,
            right_outputs,
            cte_scan_names,
            output_indexes,
        };

        let mut new_expr = SExpr::create_binary(
            Arc::new(union_plan.into()),
            Arc::new(left_expr),
            Arc::new(right_expr),
        );

        if distinct {
            let columns = new_bind_context.all_column_bindings().to_vec();
            new_expr = self.bind_distinct(
                left_span,
                &mut new_bind_context,
                &columns,
                &mut HashMap::new(),
                new_expr,
            )?;
        }

        Ok((new_expr, new_bind_context))
    }

    pub fn bind_intersect(
        &mut self,
        left_span: Span,
        right_span: Span,
        left_context: BindContext,
        right_context: BindContext,
        left_expr: SExpr,
        right_expr: SExpr,
    ) -> Result<(SExpr, BindContext)> {
        self.bind_intersect_or_except(
            left_span,
            right_span,
            left_context,
            right_context,
            left_expr,
            right_expr,
            JoinType::LeftSemi,
        )
    }

    pub fn bind_except(
        &mut self,
        left_span: Span,
        right_span: Span,
        left_context: BindContext,
        right_context: BindContext,
        left_expr: SExpr,
        right_expr: SExpr,
    ) -> Result<(SExpr, BindContext)> {
        self.bind_intersect_or_except(
            left_span,
            right_span,
            left_context,
            right_context,
            left_expr,
            right_expr,
            JoinType::LeftAnti,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn bind_intersect_or_except(
        &mut self,
        left_span: Span,
        right_span: Span,
        mut left_context: BindContext,
        right_context: BindContext,
        left_expr: SExpr,
        right_expr: SExpr,
        join_type: JoinType,
    ) -> Result<(SExpr, BindContext)> {
        let columns = left_context.all_column_bindings().to_vec();
        let left_expr = self.bind_distinct(
            left_span,
            &mut left_context,
            &columns,
            &mut HashMap::new(),
            left_expr,
        )?;
        let mut left_conditions = Vec::with_capacity(left_context.columns.len());
        let mut right_conditions = Vec::with_capacity(right_context.columns.len());
        assert_eq!(left_context.columns.len(), right_context.columns.len());
        for (left_column, right_column) in left_context
            .columns
            .iter()
            .zip(right_context.columns.iter())
        {
            left_conditions.push(
                BoundColumnRef {
                    span: left_span,
                    column: left_column.clone(),
                }
                .into(),
            );
            right_conditions.push(
                BoundColumnRef {
                    span: right_span,
                    column: right_column.clone(),
                }
                .into(),
            );
        }
        let is_null_equal = (0..left_conditions.len()).collect();
        let join_conditions = JoinConditions {
            left_conditions,
            right_conditions,
            non_equi_conditions: vec![],
            other_conditions: vec![],
        };
        let s_expr = self.bind_join_with_type(
            join_type,
            join_conditions,
            left_expr,
            right_expr,
            is_null_equal,
            None,
        )?;
        left_context
            .cte_context
            .set_cte_context(right_context.cte_context);
        Ok((s_expr, left_context))
    }

    #[allow(clippy::type_complexity)]
    #[allow(clippy::too_many_arguments)]
    fn coercion_union_type(
        &mut self,
        left_span: Span,
        right_span: Span,
        left_bind_context: BindContext,
        right_bind_context: BindContext,
        coercion_types: Vec<DataType>,
    ) -> Result<(
        BindContext,
        Vec<(IndexType, Option<ScalarExpr>)>,
        Vec<(IndexType, Option<ScalarExpr>)>,
    )> {
        let mut left_outputs = Vec::with_capacity(left_bind_context.columns.len());
        let mut right_outputs = Vec::with_capacity(right_bind_context.columns.len());
        let mut new_bind_context = BindContext::new();

        new_bind_context
            .cte_context
            .set_cte_context(right_bind_context.cte_context);

        for (idx, (left_col, right_col)) in left_bind_context
            .columns
            .iter()
            .zip(right_bind_context.columns.iter())
            .enumerate()
        {
            if *left_col.data_type != coercion_types[idx] {
                let left_coercion_expr = CastExpr {
                    span: left_span,
                    is_try: false,
                    argument: Box::new(
                        BoundColumnRef {
                            span: left_span,
                            column: left_col.clone(),
                        }
                        .into(),
                    ),
                    target_type: Box::new(coercion_types[idx].clone()),
                };
                left_outputs.push((
                    left_col.index,
                    Some(ScalarExpr::CastExpr(left_coercion_expr)),
                ));
            } else {
                left_outputs.push((left_col.index, None));
            }

            if *right_col.data_type != coercion_types[idx] {
                let right_coercion_expr = CastExpr {
                    span: right_span,
                    is_try: false,
                    argument: Box::new(
                        BoundColumnRef {
                            span: right_span,
                            column: right_col.clone(),
                        }
                        .into(),
                    ),
                    target_type: Box::new(coercion_types[idx].clone()),
                };
                right_outputs.push((
                    right_col.index,
                    Some(ScalarExpr::CastExpr(right_coercion_expr)),
                ));
            } else {
                right_outputs.push((right_col.index, None));
            }

            let column_binding = self.create_derived_column_binding(
                left_col.column_name.clone(),
                coercion_types[idx].clone(),
                None,
            );
            new_bind_context.add_column_binding(column_binding);
        }

        Ok((new_bind_context, left_outputs, right_outputs))
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn analyze_lazy_materialization(
        &self,
        bind_context: &BindContext,
        stmt: &SelectStmt,
        scalar_items: &HashMap<IndexType, ScalarItem>,
        select_list: &SelectList,
        where_scalar: &Option<ScalarExpr>,
        order_by: &[OrderItem],
        limit: usize,
    ) -> Result<()> {
        // Only simple single table queries with limit are supported.
        // e.g.
        // SELECT ... FROM t WHERE ... LIMIT ...
        // SELECT ... FROM t WHERE ... ORDER BY ... LIMIT ...
        if stmt.group_by.is_some()
            || stmt.having.is_some()
            || stmt.distinct
            || stmt.qualify.is_some()
            || !bind_context.aggregate_info.group_items.is_empty()
            || !bind_context.aggregate_info.aggregate_functions.is_empty()
            || !bind_context.srf_info.srfs.is_empty()
        {
            return Ok(());
        }

        let mut metadata = self.metadata.write();
        if metadata.tables().len() != 1 {
            // Only support single table query.
            return Ok(());
        }

        for order_by_item in order_by {
            let column = metadata.column(order_by_item.index);
            // If order by contains derived column or virtual column,
            // limit can't be pushed down, so there's no need row fetcher.
            if matches!(
                column,
                ColumnEntry::DerivedColumn(_) | ColumnEntry::VirtualColumn(_)
            ) {
                return Ok(());
            }
        }

        // As we don't if this is subquery, we need add required cols to metadata's non_lazy_columns,
        // so if the inner query not match the lazy materialized but outer query matched, we can prevent
        // the cols that inner query required not be pruned when analyze outer query.
        {
            let f = |scalar: &ScalarExpr| matches!(scalar, ScalarExpr::WindowFunction(_));
            let mut finder = Finder::new(&f);
            let mut non_lazy_cols = HashSet::new();

            for s in select_list.items.iter() {
                // The TableScan's schema uses name_mapping to prune columns,
                // all lazy columns will be skipped to add to name_mapping in TableScan.
                // When build physical window plan, if window's order by or partition by provided,
                // we need create a `EvalScalar` for physical window inputs, so we should keep the window
                // used cols not be pruned.
                finder.reset_finder();
                finder.visit(&s.scalar)?;
                for scalar in finder.scalars() {
                    non_lazy_cols.extend(scalar.used_columns())
                }
            }
            metadata.add_non_lazy_columns(non_lazy_cols);
        }

        let limit_threadhold = self.ctx.get_settings().get_lazy_read_threshold()? as usize;

        let where_cols = where_scalar
            .as_ref()
            .map(|w| w.used_columns())
            .unwrap_or_default();

        if limit == 0 || limit > limit_threadhold || (order_by.is_empty() && where_cols.is_empty())
        {
            return Ok(());
        }

        if !metadata
            .table(0)
            .table()
            .supported_internal_column(ROW_ID_COLUMN_ID)
        {
            return Ok(());
        }

        let cols = metadata.columns();

        let virtual_cols = cols
            .iter()
            .filter(|col| matches!(col, ColumnEntry::VirtualColumn(_)))
            .map(|col| col.index())
            .collect::<Vec<_>>();

        if !virtual_cols.is_empty() {
            // Virtual columns are not supported now.
            return Ok(());
        }

        let mut order_by_cols = HashSet::with_capacity(order_by.len());
        for o in order_by {
            if let Some(scalar) = scalar_items.get(&o.index) {
                let cols = scalar.scalar.used_columns();
                order_by_cols.extend(cols);
            } else {
                // Is a col ref not appears in select list.
                order_by_cols.insert(o.index);
            }
        }

        let mut non_lazy_cols = order_by_cols;
        non_lazy_cols.extend(where_cols);

        let mut select_cols = HashSet::with_capacity(select_list.items.len());
        for s in select_list.items.iter() {
            if let ScalarExpr::WindowFunction(_) = &s.scalar {
                continue;
            } else {
                select_cols.extend(s.scalar.used_columns())
            }
        }

        // If there are derived columns, we can't use lazy materialization.
        // (As the derived columns may come from a CTE, the rows fetcher can't know where to fetch the data.)
        if select_cols
            .iter()
            .any(|col| matches!(metadata.column(*col), ColumnEntry::DerivedColumn(_)))
        {
            return Ok(());
        }

        let internal_cols = cols
            .iter()
            .filter(|col| matches!(col, ColumnEntry::InternalColumn(_)))
            .map(|col| col.index())
            .collect::<HashSet<_>>();

        // add internal_cols to non_lazy_cols
        non_lazy_cols.extend(internal_cols);

        // add previous(subquery) stored non_lazy_columns to non_lazy_cols
        non_lazy_cols.extend(metadata.non_lazy_columns());

        let lazy_cols = select_cols.difference(&non_lazy_cols).copied().collect();
        metadata.add_lazy_columns(lazy_cols);

        // Single table, the table index is 0.
        let table_index = 0;
        if metadata.row_id_index_by_table_index(table_index).is_none() {
            let internal_column = INTERNAL_COLUMN_FACTORY
                .get_internal_column(ROW_ID_COL_NAME)
                .unwrap();
            let index = metadata.add_internal_column(table_index, internal_column);
            metadata.set_table_row_id_index(table_index, index);
        }

        Ok(())
    }
}
