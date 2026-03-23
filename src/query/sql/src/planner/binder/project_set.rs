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
use std::mem;
use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::FunctionKind;
use databend_common_expression::types::DataType;
use databend_common_functions::BUILTIN_FUNCTIONS;

use crate::BindContext;
use crate::Binder;
use crate::ColumnBinding;
use crate::MetadataRef;
use crate::ScalarExpr;
use crate::Visibility;
use crate::binder::ColumnBindingBuilder;
use crate::binder::aggregate::AggregateRewriter;
use crate::binder::select::SelectList;
use crate::format_scalar;
use crate::optimizer::ir::SExpr;
use crate::plans::BoundColumnRef;
use crate::plans::FunctionCall;
use crate::plans::ProjectSet;
use crate::plans::ScalarItem;
use crate::plans::VisitorMut;
use crate::plans::walk_expr_mut;

#[derive(Default, Clone, PartialEq, Eq, Debug)]
pub struct SetReturningInfo {
    /// Set-returning functions.
    pub srfs: Vec<ScalarItem>,
    /// Mapping: (Set-returning function display name) -> (index of Set-returning function in `srfs`)
    /// This is used to find a Set-returning function in current context.
    pub srfs_map: HashMap<String, usize>,
    /// The lazy index of Set-returning functions in `srfs`.
    /// Those set-returning function's argument contains aggregate functions or group by items.
    /// Build a lazy `ProjectSet` plan after the `Aggregate` plan.
    pub lazy_srf_set: HashSet<usize>,
}

/// Analyze Set-returning functions and create derived columns.
pub(crate) struct SetReturningAnalyzer<'a> {
    bind_context: &'a mut BindContext,
    metadata: MetadataRef,
}

struct DeferredAggregateRewriter<'a> {
    bind_context: &'a mut BindContext,
    metadata: MetadataRef,
}

// Keep SRF output type as tuple in metadata even if a single field was extracted.
fn normalize_srf_return_type(data_type: DataType) -> DataType {
    if data_type.as_tuple().is_some() {
        data_type
    } else {
        DataType::Tuple(vec![data_type])
    }
}

impl<'a> SetReturningAnalyzer<'a> {
    pub(crate) fn new(bind_context: &'a mut BindContext, metadata: MetadataRef) -> Self {
        Self {
            bind_context,
            metadata,
        }
    }

    fn rewrite_aggregate_expr(&mut self, expr: &mut ScalarExpr) -> Result<()> {
        let mut rewriter = DeferredAggregateRewriter {
            bind_context: self.bind_context,
            metadata: self.metadata.clone(),
        };
        rewriter.visit(expr)
    }
}

impl DeferredAggregateRewriter<'_> {
    fn find_registered_aggregate_scalar(&self, index: crate::Symbol) -> Option<ScalarExpr> {
        self.bind_context
            .aggregate_info
            .aggregate_calls_for_plan()
            .into_iter()
            .find(|item| item.index == index)
            .map(|item| item.scalar)
    }
}

impl<'a> VisitorMut<'a> for DeferredAggregateRewriter<'a> {
    fn visit(&mut self, expr: &'a mut ScalarExpr) -> Result<()> {
        match expr {
            ScalarExpr::AggregateFunction(_) | ScalarExpr::UDAFCall(_) => {
                let mut rewritten = expr.clone();
                AggregateRewriter::rewrite_expr(
                    &mut self.bind_context.aggregate_info,
                    self.metadata.clone(),
                    &mut rewritten,
                )?;

                if let ScalarExpr::BoundColumnRef(column_ref) = &rewritten {
                    if let Some(scalar) =
                        self.find_registered_aggregate_scalar(column_ref.column.index)
                    {
                        *expr = scalar;
                        return Ok(());
                    }
                }

                *expr = rewritten;
                Ok(())
            }
            _ => walk_expr_mut(self, expr),
        }
    }
}

impl<'a> VisitorMut<'a> for SetReturningAnalyzer<'a> {
    fn visit(&mut self, expr: &'a mut ScalarExpr) -> Result<()> {
        if let ScalarExpr::FunctionCall(func) = expr {
            if BUILTIN_FUNCTIONS
                .get_property(&func.func_name)
                .map(|property| property.kind == FunctionKind::SRF)
                .unwrap_or(false)
            {
                let mut replaced_args = Vec::with_capacity(func.arguments.len());
                for arg in func.arguments.iter() {
                    let mut arg = arg.clone();
                    self.rewrite_aggregate_expr(&mut arg)?;
                    replaced_args.push(arg);
                }

                let replaced_expr: ScalarExpr = FunctionCall {
                    span: func.span,
                    func_name: func.func_name.clone(),
                    params: func.params.clone(),
                    arguments: replaced_args,
                }
                .into();

                let srf_display_name = format_scalar(&replaced_expr);

                let srf_info = &mut self.bind_context.srf_info;
                if let Some(column_binding) =
                    find_replaced_set_returning_function(srf_info, &srf_display_name)
                {
                    *expr = BoundColumnRef {
                        span: None,
                        column: column_binding,
                    }
                    .into();
                    return Ok(());
                }

                let data_type = normalize_srf_return_type(replaced_expr.data_type()?);
                let index = self
                    .metadata
                    .write()
                    .add_derived_column(srf_display_name.clone(), data_type.clone());

                // Add the srf to bind context, build ProjectSet plan later.
                self.bind_context.srf_info.srfs.push(ScalarItem {
                    index,
                    scalar: replaced_expr.clone(),
                });
                self.bind_context.srf_info.srfs_map.insert(
                    srf_display_name.clone(),
                    self.bind_context.srf_info.srfs.len() - 1,
                );

                let column_binding = ColumnBindingBuilder::new(
                    srf_display_name,
                    index,
                    Box::new(data_type),
                    Visibility::Visible,
                )
                .build();

                *expr = BoundColumnRef {
                    span: None,
                    column: column_binding,
                }
                .into();

                return Ok(());
            }
        }

        walk_expr_mut(self, expr)
    }
}

/// Check whether the argument of Set-returning functions contains aggregation function or group item.
/// If true, rewrite aggregation function as a BoundColumnRef, and build a lazy `ProjectSet` plan
struct SetReturningRewriter<'a> {
    bind_context: &'a mut BindContext,
    is_lazy_srf: bool,
}

impl<'a> SetReturningRewriter<'a> {
    fn new(bind_context: &'a mut BindContext) -> Self {
        Self {
            bind_context,
            is_lazy_srf: false,
        }
    }
}

impl<'a> VisitorMut<'a> for SetReturningRewriter<'a> {
    fn visit(&mut self, expr: &'a mut ScalarExpr) -> Result<()> {
        if self.bind_context.aggregate_info.contains_group_item(expr) {
            self.is_lazy_srf = true;
        }

        if expr.is_aggregate() {
            self.is_lazy_srf = true;
            let span = expr.span();
            AggregateRewriter::rewrite_existing_expr(
                &self.bind_context.aggregate_info,
                expr,
                "ProjectSet rewrite expected aggregate functions to be pre-registered",
            )
            .map_err(|err| ErrorCode::Internal(err.message()))?;

            if let ScalarExpr::BoundColumnRef(column_ref) = expr {
                column_ref.span = span;
                column_ref.column.visibility = Visibility::InVisible;
            }
            return Ok(());
        }

        walk_expr_mut(self, expr)
    }
}

impl Binder {
    /// Analyze project sets in select clause.
    /// See [`SetReturningAnalyzer`] for more details.
    pub(crate) fn analyze_project_set_select(
        &mut self,
        bind_context: &mut BindContext,
        select_list: &mut SelectList,
    ) -> Result<()> {
        let mut analyzer = SetReturningAnalyzer::new(bind_context, self.metadata.clone());
        for item in select_list.items.iter_mut() {
            analyzer.visit(&mut item.scalar)?;
        }

        Ok(())
    }

    /// Rewrite the argument of project sets and find lazy srfs.
    /// See [`SetReturningRewriter`] for more details.
    pub(crate) fn rewrite_project_set_select(
        &mut self,
        bind_context: &mut BindContext,
    ) -> Result<()> {
        let mut srf_info = mem::take(&mut bind_context.srf_info);
        let mut rewriter = SetReturningRewriter::new(bind_context);
        for srf_item in srf_info.srfs.iter_mut() {
            let srf_display_name = format_scalar(&srf_item.scalar);
            rewriter.is_lazy_srf = false;
            rewriter.visit(&mut srf_item.scalar)?;

            // If the argument contains aggregation function or group item.
            // add the srf index to lazy set.
            if rewriter.is_lazy_srf {
                if let Some(index) = srf_info.srfs_map.get(&srf_display_name) {
                    srf_info.lazy_srf_set.insert(*index);
                }
            }
        }
        bind_context.srf_info = srf_info;

        Ok(())
    }

    pub(crate) fn bind_project_set(
        &mut self,
        bind_context: &mut BindContext,
        child: SExpr,
        is_lazy: bool,
    ) -> Result<SExpr> {
        let srf_len = if is_lazy {
            bind_context.srf_info.lazy_srf_set.len()
        } else {
            bind_context.srf_info.srfs.len() - bind_context.srf_info.lazy_srf_set.len()
        };
        if srf_len == 0 {
            return Ok(child);
        }

        // Build a ProjectSet Plan.
        let mut srfs = Vec::with_capacity(srf_len);
        for (i, srf) in bind_context.srf_info.srfs.iter().enumerate() {
            let is_lazy_srf = bind_context.srf_info.lazy_srf_set.contains(&i);
            if (is_lazy && is_lazy_srf) || (!is_lazy && !is_lazy_srf) {
                srfs.push(srf.clone());
            }
        }

        let project_set = ProjectSet { srfs };
        let new_expr = SExpr::create_unary(Arc::new(project_set.into()), Arc::new(child));

        Ok(new_expr)
    }
}

/// Replace [`SetReturningFunction`] with a [`ColumnBinding`] if the function is already replaced.
pub fn find_replaced_set_returning_function(
    srf_info: &SetReturningInfo,
    srf_display_name: &str,
) -> Option<ColumnBinding> {
    srf_info.srfs_map.get(srf_display_name).map(|i| {
        // This expression is already replaced.
        let scalar_item = &srf_info.srfs[*i];
        let data_type = normalize_srf_return_type(scalar_item.scalar.data_type().unwrap());
        ColumnBindingBuilder::new(
            srf_display_name.to_string(),
            scalar_item.index,
            Box::new(data_type),
            Visibility::Visible,
        )
        .build()
    })
}
