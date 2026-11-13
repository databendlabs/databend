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
use crate::binder::GroupingScopeState;
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

/// Where a set-returning function is evaluated, relative to the aggregate of
/// its SELECT, by where it is bound.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum SrfPhase {
    /// Below the aggregate, if any: bound in a key, an aggregate argument,
    /// WHERE, or a SELECT without aggregate.
    Input,
    /// Above the aggregate, in a lazy `ProjectSet`: bound after the
    /// aggregate of a grouped SELECT, even over constants.
    Output,
}

/// A set-returning function and where it is evaluated.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct SrfItem {
    pub item: ScalarItem,
    pub phase: SrfPhase,
}

#[derive(Default, Clone, PartialEq, Eq, Debug)]
pub struct SetReturningInfo {
    /// Set-returning functions.
    pub srfs: Vec<SrfItem>,
    /// Mapping: (display name, phase) -> index in `srfs`, to find a
    /// set-returning function in the current context. A function bound in
    /// both phases, e.g. `unnest([1, 2])` as a key and in the SELECT list, is
    /// evaluated twice.
    srfs_map: HashMap<(String, SrfPhase), usize>,
}

impl SetReturningInfo {
    /// Whether a set-returning function is evaluated above the aggregate.
    pub fn has_lazy(&self) -> bool {
        self.srfs.iter().any(|srf| srf.phase == SrfPhase::Output)
    }
}

/// Analyze Set-returning functions and create derived columns.
pub(crate) struct SetReturningAnalyzer<'a> {
    bind_context: &'a mut BindContext,
    metadata: MetadataRef,
}

struct DeferredAggregateRewriter<'a> {
    bind_context: &'a BindContext,
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
        };
        rewriter.visit(expr)
    }
}

impl DeferredAggregateRewriter<'_> {
    fn find_registered_aggregate_scalar(&self, index: crate::Symbol) -> Option<ScalarExpr> {
        self.bind_context
            .aggregate_info
            .aggregate_calls()
            .find(|item| item.index == index)
            .map(|item| item.scalar.clone())
    }
}

impl<'a> VisitorMut<'a> for DeferredAggregateRewriter<'a> {
    fn visit(&mut self, expr: &'a mut ScalarExpr) -> Result<()> {
        match expr {
            ScalarExpr::BoundColumnRef(column) => {
                if let Some(aggregate) = self.find_registered_aggregate_scalar(column.column.index)
                {
                    *expr = aggregate;
                }
                Ok(())
            }
            ScalarExpr::AggregateFunction(_) | ScalarExpr::UDAFCall(_) => {
                // An aggregate function bound after the aggregate is already
                // registered, see `TypeChecker::rewrite_grouped_output`; one
                // that is not is evaluated before the aggregate.
                let mut rewritten = expr.clone();
                AggregateRewriter::rewrite_existing_expr(
                    &self.bind_context.aggregate_info,
                    &mut rewritten,
                    "set-returning functions evaluated before the aggregate can't contain aggregate functions",
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
                    return_type: func.return_type.clone(),
                }
                .into();

                let srf_display_name = format_scalar(&replaced_expr);

                // Even a constant SRF belongs above Aggregate when it is
                // bound as a grouped output, not as a key/aggregate input.
                let phase = match self.bind_context.grouping_scope_state() {
                    Some(GroupingScopeState::Active) => SrfPhase::Output,
                    _ => SrfPhase::Input,
                };
                let srf_info = &mut self.bind_context.srf_info;
                if let Some(column_binding) =
                    find_replaced_set_returning_function(srf_info, &srf_display_name, phase)
                {
                    *expr = BoundColumnRef {
                        span: None,
                        column: column_binding,
                    }
                    .into();
                    return Ok(());
                }

                let data_type = normalize_srf_return_type(replaced_expr.data_type().into_owned());
                let index = self
                    .metadata
                    .write()
                    .add_derived_column(srf_display_name.clone(), data_type.clone());

                // Add the srf to bind context, build ProjectSet plan later.
                let srf_info = &mut self.bind_context.srf_info;
                srf_info
                    .srfs_map
                    .insert((srf_display_name.clone(), phase), srf_info.srfs.len());
                srf_info.srfs.push(SrfItem {
                    item: ScalarItem {
                        index,
                        scalar: replaced_expr.clone(),
                    },
                    phase,
                });

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

/// Rewrite the aggregate functions in the arguments of set-returning
/// functions as references to their outputs.
struct SetReturningRewriter<'a> {
    bind_context: &'a mut BindContext,
}

impl<'a> VisitorMut<'a> for SetReturningRewriter<'a> {
    fn visit(&mut self, expr: &'a mut ScalarExpr) -> Result<()> {
        if expr.is_aggregate() {
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

    /// Rewrite the arguments of project sets, see [`SetReturningRewriter`].
    /// Whether one is lazy is its phase, known when it is registered.
    pub(crate) fn rewrite_project_set_select(
        &mut self,
        bind_context: &mut BindContext,
    ) -> Result<()> {
        let mut srf_info = mem::take(&mut bind_context.srf_info);
        let mut rewriter = SetReturningRewriter { bind_context };
        for srf in srf_info.srfs.iter_mut() {
            rewriter.visit(&mut srf.item.scalar)?;
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
        let phase = if is_lazy {
            SrfPhase::Output
        } else {
            SrfPhase::Input
        };
        let srfs = bind_context
            .srf_info
            .srfs
            .iter()
            .filter(|srf| srf.phase == phase)
            .map(|srf| srf.item.clone())
            .collect::<Vec<_>>();
        if srfs.is_empty() {
            return Ok(child);
        }

        // Build a ProjectSet Plan.

        let project_set = ProjectSet { srfs };
        let new_expr = SExpr::create_unary(Arc::new(project_set.into()), Arc::new(child));

        Ok(new_expr)
    }
}

/// Replace [`SetReturningFunction`] with a [`ColumnBinding`] if the function is already replaced.
pub fn find_replaced_set_returning_function(
    srf_info: &SetReturningInfo,
    srf_display_name: &str,
    phase: SrfPhase,
) -> Option<ColumnBinding> {
    srf_info
        .srfs_map
        .get(&(srf_display_name.to_string(), phase))
        .map(|i| {
            // This expression is already replaced.
            let scalar_item = &srf_info.srfs[*i].item;
            let data_type = normalize_srf_return_type(scalar_item.scalar.data_type().into_owned());
            ColumnBindingBuilder::new(
                srf_display_name.to_string(),
                scalar_item.index,
                Box::new(data_type),
                Visibility::Visible,
            )
            .build()
        })
}
