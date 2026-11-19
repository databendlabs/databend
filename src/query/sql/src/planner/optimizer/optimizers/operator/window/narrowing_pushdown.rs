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

use async_trait::async_trait;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;

use crate::ColumnSet;
use crate::Symbol;
use crate::Visibility;
use crate::binder::ColumnBindingBuilder;
use crate::format_scalar;
use crate::optimizer::Optimizer;
use crate::optimizer::OptimizerContext;
use crate::optimizer::ir::SExpr;
use crate::plans::BoundColumnRef;
use crate::plans::EvalScalar;
use crate::plans::Operator;
use crate::plans::RelOperator;
use crate::plans::ScalarExpr;
use crate::plans::ScalarItem;

/// Functions that extract a part of a wide value and never raise a runtime
/// error. Evaluating them on rows that a later filter discards therefore
/// cannot change the query outcome, and their result is never wider than the
/// value they read.
const NARROWING_FUNCTIONS: &[&str] = &[
    "get",
    "get_ignore_case",
    "get_string",
    "get_by_keypath",
    "get_by_keypath_string",
    "json_extract_path_text",
    "as_boolean",
    "as_integer",
    "as_float",
    "as_string",
];

/// Move narrowing extractions over wide columns below the window that would
/// otherwise carry the whole value.
///
/// Input:   EvalScalar(... f(wide_col) ...)
///           \
///            [Filter]
///             \
///              Window | WindowGroup
///               \
///                *
///
/// Output:  EvalScalar(... #new ...)
///           \
///            [Filter]
///             \
///              Window | WindowGroup
///               \
///                EvalScalar(#new <- f(wide_col))
///                 \
///                  *
///
/// The binder evaluates only window arguments and keys below a window; every
/// other select item stays above it, so `message:a:b::INT` next to
/// `row_number() OVER (...)` drags the whole VARIANT through the partition
/// shuffle, buffer, spill and restore. When nothing above the window reads
/// `wide_col` otherwise, evaluating the extraction first lets column pruning
/// drop `wide_col` from the window input.
///
/// Logical `EvalScalar` passes every input column through, so whether
/// `wide_col` is still read above is only known from the query's output
/// columns. The pass walks the tree top-down carrying that required set.
///
/// Virtual columns take precedence: when the binder resolves a static JSON
/// path on a fuse table with `enable_virtual_column` to a virtual column, the
/// narrow value is produced by the scan and the source column never reaches
/// the window, so this pass finds nothing to move. It only steps in when the
/// source column still flows through the window: tables without the option,
/// VARIANT values computed by expressions, CTEs or external sources, access
/// forms the virtual column binder does not cover, and non-VARIANT wide
/// columns such as String or Array.
pub struct NarrowingPushdownOptimizer {
    ctx: Arc<OptimizerContext>,
    output_columns: Option<HashSet<Symbol>>,
}

impl NarrowingPushdownOptimizer {
    pub fn new(ctx: Arc<OptimizerContext>, output_columns: Option<HashSet<Symbol>>) -> Self {
        Self {
            ctx,
            output_columns,
        }
    }

    #[recursive::recursive]
    fn rewrite(&self, expr: &SExpr, required: ColumnSet) -> Result<SExpr> {
        let mut expr = expr.clone();
        if let RelOperator::EvalScalar(eval_scalar) = expr.plan() {
            let child = expr.child(0)?;
            if let Some((eval_scalar, child)) =
                self.narrow_window_input(eval_scalar, &required, child)?
            {
                expr = SExpr::create_unary(Arc::new(eval_scalar.into()), Arc::new(child));
            }
        }

        let mut children = Vec::with_capacity(expr.arity());
        for (index, child) in expr.children().enumerate() {
            let child_required = child_required(&expr, &required, index, child)?;
            children.push(Arc::new(self.rewrite(child, child_required)?));
        }
        Ok(expr.replace_children(children))
    }

    /// `required` is what the parent reads from `eval_scalar`. Returns the
    /// rewritten `EvalScalar` and child when something moved.
    fn narrow_window_input(
        &self,
        eval_scalar: &EvalScalar,
        required: &ColumnSet,
        child: &SExpr,
    ) -> Result<Option<(EvalScalar, SExpr)>> {
        let (filter, window_expr) = match child.plan() {
            RelOperator::Filter(filter) => (Some(filter.clone()), child.child(0)?),
            _ => (None, child),
        };
        if !matches!(
            window_expr.plan(),
            RelOperator::Window(_) | RelOperator::WindowGroup(_)
        ) {
            return Ok(None);
        }
        let window_input = window_expr.child(0)?;
        let available = window_input
            .derive_relational_prop()?
            .output_columns
            .clone();

        // Items the parent does not read are pruned later and must neither
        // move nor pin a wide column.
        let is_live = |item: &ScalarItem| required.contains(&item.index);
        let pass_through: ColumnSet = required
            .iter()
            .filter(|index| !eval_scalar.items.iter().any(|item| item.index == **index))
            .copied()
            .collect();

        let mut collector = CandidateCollector {
            available: &available,
            candidates: Vec::new(),
            other_used: pass_through,
        };
        for item in eval_scalar.items.iter().filter(|item| is_live(item)) {
            collector.collect(&item.scalar);
        }
        if let Some(filter) = &filter {
            for predicate in &filter.predicates {
                collector.collect(predicate);
            }
        }
        let CandidateCollector {
            candidates,
            other_used,
            ..
        } = collector;
        // Only worth it when the wide source column can be pruned afterwards.
        let candidates: Vec<ScalarExpr> = candidates
            .into_iter()
            .filter(|candidate| candidate.used_columns().is_disjoint(&other_used))
            .collect();
        if candidates.is_empty() {
            return Ok(None);
        }

        let metadata = self.ctx.get_metadata();
        let mut pushed_items = Vec::new();
        let mut replacements: HashMap<ScalarExpr, ScalarExpr> = HashMap::new();
        for candidate in candidates {
            if replacements.contains_key(&candidate) {
                continue;
            }
            let data_type = candidate.data_type().into_owned();
            let alias = format_scalar(&candidate);
            let index = metadata
                .write()
                .add_derived_column(alias.clone(), data_type.clone());
            let column =
                ColumnBindingBuilder::new(alias, index, Box::new(data_type), Visibility::Visible)
                    .build();
            replacements.insert(
                candidate.clone(),
                ScalarExpr::BoundColumnRef(BoundColumnRef { span: None, column }),
            );
            pushed_items.push(ScalarItem {
                scalar: candidate,
                index,
            });
        }

        let mut eval_scalar = eval_scalar.clone();
        for item in eval_scalar.items.iter_mut().filter(|item| is_live(item)) {
            replace_candidates(&mut item.scalar, &replacements);
        }
        let filter = filter.map(|mut filter| {
            for predicate in &mut filter.predicates {
                replace_candidates(predicate, &replacements);
            }
            filter
        });

        let pushed = EvalScalar {
            items: pushed_items,
        };
        let new_window = match window_expr.plan() {
            RelOperator::WindowGroup(group) => {
                // The group evaluates its scalar inputs in one EvalScalar
                // below the partition sort; the extractions join them.
                let mut group = group.clone();
                group.scalar_items.extend(pushed.items);
                SExpr::create_unary(Arc::new(group.into()), Arc::new(window_input.clone()))
            }
            _ => {
                // The partition shuffle and spill happen in the Sort below the
                // window, so the extraction has to sit below that Sort.
                let new_input = match window_input.plan() {
                    RelOperator::Sort(_) => window_input.replace_children(vec![Arc::new(
                        prepend_eval_scalar(window_input.child(0)?, pushed)?,
                    )]),
                    _ => prepend_eval_scalar(window_input, pushed)?,
                };
                window_expr.replace_children(vec![Arc::new(new_input)])
            }
        };

        let new_child = match filter {
            Some(filter) => SExpr::create_unary(Arc::new(filter.into()), Arc::new(new_window)),
            None => new_window,
        };
        Ok(Some((eval_scalar, new_child)))
    }
}

#[async_trait]
impl Optimizer for NarrowingPushdownOptimizer {
    fn name(&self) -> String {
        "NarrowingPushdownOptimizer".to_string()
    }

    async fn optimize(&mut self, s_expr: SExpr) -> Result<SExpr> {
        let Some(output_columns) = &self.output_columns else {
            return Ok(s_expr);
        };
        let required: ColumnSet = output_columns.iter().copied().collect();
        self.rewrite(&s_expr, required)
    }
}

/// Columns a child has to produce for its parent: what the parent passes on
/// plus what the parent itself reads. Operators that define columns from
/// expressions evaluated below them (EvalScalar, Window, WindowGroup) read the
/// evaluated column, not its sources, mirroring the physical column pruning.
/// Operators without a modelled column flow keep everything, which only
/// forgoes rewrites below them.
fn child_required(
    parent: &SExpr,
    required: &ColumnSet,
    child_index: usize,
    child: &SExpr,
) -> Result<ColumnSet> {
    let child_output = &child.derive_relational_prop()?.output_columns;
    let mut result = ColumnSet::new();
    match parent.plan() {
        RelOperator::EvalScalar(eval_scalar) => {
            result.extend(
                required
                    .iter()
                    .filter(|index| !eval_scalar.items.iter().any(|item| item.index == **index)),
            );
            for item in &eval_scalar.items {
                if required.contains(&item.index) {
                    item.scalar.collect_used_columns(&mut result);
                }
            }
        }
        RelOperator::Window(window) => {
            result.extend(required.iter().filter(|index| **index != window.index));
            window.function.collect_used_columns(&mut result);
            for item in &window.arguments {
                result.insert(item.index);
            }
            for item in &window.partition_by {
                result.insert(item.index);
            }
            for item in &window.order_by {
                result.insert(item.order_by_item.index);
            }
        }
        RelOperator::WindowGroup(group) => {
            result.extend(required.iter());
            for window in &group.windows {
                result.remove(&window.index);
                window.function.collect_used_columns(&mut result);
                for item in &window.arguments {
                    result.insert(item.index);
                }
                for item in &window.partition_by {
                    result.insert(item.index);
                }
                for item in &window.order_by {
                    result.insert(item.order_by_item.index);
                }
            }
            for item in &group.scalar_items {
                result.remove(&item.index);
            }
            for item in &group.scalar_items {
                item.scalar.collect_used_columns(&mut result);
            }
        }
        RelOperator::Sort(sort) => {
            result.extend(required.iter());
            result.extend(sort.items.iter().map(|item| item.index));
            if let Some(window) = &sort.window_partition {
                result.extend(window.partition_by.iter().map(|item| item.index));
                window.func.collect_used_columns(&mut result);
            }
        }
        RelOperator::Filter(_)
        | RelOperator::Join(_)
        | RelOperator::Aggregate(_)
        | RelOperator::Limit(_)
        | RelOperator::ProjectSet(_)
        | RelOperator::Udf(_)
        | RelOperator::AsyncFunction(_) => {
            result.extend(required.iter());
            for scalar in parent.plan().scalar_expr_iter() {
                scalar.collect_used_columns(&mut result);
            }
        }
        _ => return Ok(child_output.clone()),
    }
    let _ = child_index;
    Ok(result.intersection(child_output).copied().collect())
}

/// Evaluate `pushed` on top of `input`. The binder already puts an
/// `EvalScalar` with the window inputs there; when the pushed items only read
/// that operator's input columns they share it, otherwise (an extraction over
/// a window input column such as `as_string(window_input)`) they stack above
/// it.
fn prepend_eval_scalar(input: &SExpr, pushed: EvalScalar) -> Result<SExpr> {
    if let RelOperator::EvalScalar(existing) = input.plan() {
        let child = input.child(0)?;
        let child_output = &child.derive_relational_prop()?.output_columns;
        let reads_only_child = pushed
            .items
            .iter()
            .all(|item| item.scalar.used_columns().is_subset(child_output));
        if reads_only_child {
            let mut merged = existing.clone();
            merged.items.extend(pushed.items);
            return Ok(SExpr::create_unary(
                Arc::new(merged.into()),
                Arc::new(child.clone()),
            ));
        }
    }
    Ok(SExpr::create_unary(
        Arc::new(pushed.into()),
        Arc::new(input.clone()),
    ))
}

struct CandidateCollector<'a> {
    available: &'a ColumnSet,
    candidates: Vec<ScalarExpr>,
    /// Columns read above the window outside of any candidate.
    other_used: ColumnSet,
}

impl CandidateCollector<'_> {
    fn collect(&mut self, scalar: &ScalarExpr) {
        if self.is_candidate(scalar) {
            self.candidates.push(scalar.clone());
            return;
        }
        match scalar {
            ScalarExpr::BoundColumnRef(column) => {
                self.other_used.insert(column.column.index);
            }
            ScalarExpr::ConstantExpr(_) | ScalarExpr::TypedConstantExpr(_, _) => {}
            ScalarExpr::FunctionCall(func) => {
                for argument in &func.arguments {
                    self.collect(argument);
                }
            }
            ScalarExpr::CastExpr(cast) => self.collect(&cast.argument),
            // Other scopes (window and aggregate functions, lambdas,
            // subqueries, UDFs) stay as they are; every column they read
            // keeps a consumer above the window.
            _ => scalar.collect_used_columns(&mut self.other_used),
        }
    }

    /// A candidate is a non-trivial narrowing extraction over wide columns
    /// that exist below the window.
    fn is_candidate(&self, scalar: &ScalarExpr) -> bool {
        !matches!(scalar, ScalarExpr::BoundColumnRef(_)) && self.is_narrowing(scalar)
    }

    fn is_narrowing(&self, scalar: &ScalarExpr) -> bool {
        match scalar {
            ScalarExpr::BoundColumnRef(column) => {
                self.available.contains(&column.column.index)
                    && is_wide_type(&column.column.data_type)
            }
            ScalarExpr::CastExpr(cast) => cast.is_try && self.is_narrowing(&cast.argument),
            ScalarExpr::FunctionCall(func) => {
                NARROWING_FUNCTIONS.contains(&func.func_name.as_str())
                    && func
                        .arguments
                        .first()
                        .is_some_and(|source| self.is_narrowing(source))
                    && func.arguments.iter().skip(1).all(|argument| {
                        matches!(
                            argument,
                            ScalarExpr::ConstantExpr(_) | ScalarExpr::TypedConstantExpr(_, _)
                        )
                    })
            }
            _ => false,
        }
    }
}

fn is_wide_type(data_type: &DataType) -> bool {
    matches!(
        data_type.remove_nullable(),
        DataType::Variant
            | DataType::String
            | DataType::Binary
            | DataType::Bitmap
            | DataType::Geometry
            | DataType::Geography
            | DataType::Array(_)
            | DataType::Map(_)
            | DataType::Tuple(_)
    )
}

fn replace_candidates(scalar: &mut ScalarExpr, replacements: &HashMap<ScalarExpr, ScalarExpr>) {
    if let Some(replacement) = replacements.get(scalar) {
        *scalar = replacement.clone();
        return;
    }
    match scalar {
        ScalarExpr::FunctionCall(func) => {
            for argument in &mut func.arguments {
                replace_candidates(argument, replacements);
            }
        }
        ScalarExpr::CastExpr(cast) => replace_candidates(&mut cast.argument, replacements),
        _ => {}
    }
}
