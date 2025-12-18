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

use databend_common_exception::Result;
use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::FunctionContext;
use databend_common_expression::type_check::check_number;
use databend_common_functions::BUILTIN_FUNCTIONS;

use crate::MetadataRef;
use crate::optimizer::ir::Matcher;
use crate::optimizer::ir::RelExpr;
use crate::optimizer::ir::SExpr;
use crate::optimizer::optimizers::rule::Rule;
use crate::optimizer::optimizers::rule::RuleID;
use crate::optimizer::optimizers::rule::TransformResult;
use crate::plans::ComparisonOp;
use crate::plans::ConstantTableScan;
use crate::plans::Filter;
use crate::plans::Operator;
use crate::plans::RelOp;
use crate::plans::RelOperator;
use crate::plans::ScalarExpr;
use crate::plans::Sort;
use crate::plans::Window;
use crate::plans::WindowFuncType;

/// Input:  Filter
///           \
///          Window
///             \
///              Sort
///
/// Output: Filter
///           \
///          Window
///             \
///              Sort(top n)
pub struct RulePushDownFilterWindowTopN {
    id: RuleID,
    metadata: MetadataRef,
    matchers: Vec<Matcher>,
}

impl RulePushDownFilterWindowTopN {
    pub fn new(metadata: MetadataRef) -> Self {
        Self {
            id: RuleID::PushDownFilterWindowTopN,
            metadata,
            matchers: vec![Matcher::MatchOp {
                op_type: RelOp::Filter,
                children: vec![Matcher::MatchOp {
                    op_type: RelOp::Window,
                    children: vec![Matcher::MatchOp {
                        op_type: RelOp::Sort,
                        children: vec![Matcher::Leaf],
                    }],
                }],
            }],
        }
    }
}

impl Rule for RulePushDownFilterWindowTopN {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformResult) -> Result<()> {
        let filter: Filter = s_expr.plan().clone().try_into()?;
        let window_expr = s_expr.child(0)?;
        let window: Window = window_expr.plan().clone().try_into()?;
        let sort_expr = window_expr.child(0)?;
        let mut sort: Sort = sort_expr.plan().clone().try_into()?;

        if !is_ranking_function(&window.function) || sort.window_partition.is_none() {
            return Ok(());
        }

        let predicates = filter
            .predicates
            .into_iter()
            .filter_map(|predicate| extract_top_n(window.index, predicate))
            .collect::<Vec<_>>();

        let Some(top_n) = predicates.into_iter().min() else {
            return Ok(());
        };

        if top_n == 0 {
            let output_columns = s_expr
                .plan()
                .derive_relational_prop(&RelExpr::with_s_expr(s_expr))?
                .output_columns
                .clone();
            let metadata = self.metadata.read();
            let mut columns = output_columns.iter().copied().collect::<Vec<_>>();
            columns.sort();
            let fields = columns
                .into_iter()
                .map(|col| DataField::new(&col.to_string(), metadata.column(col).data_type()))
                .collect::<Vec<_>>();
            let empty_scan =
                ConstantTableScan::new_empty_scan(DataSchemaRefExt::create(fields), output_columns);
            let result = SExpr::create_leaf(Arc::new(RelOperator::ConstantTableScan(empty_scan)));
            state.add_result(result);
            return Ok(());
        }

        sort.window_partition.as_mut().unwrap().top = Some(top_n);

        let mut result = SExpr::create_unary(
            s_expr.plan.clone(),
            SExpr::create_unary(
                window_expr.plan.clone(),
                sort_expr.replace_plan(Arc::new(sort.into())),
            ),
        );
        result.set_applied_rule(&self.id);

        state.add_result(result);

        Ok(())
    }

    fn matchers(&self) -> &[Matcher] {
        &self.matchers
    }
}

fn extract_top_n(column: usize, predicate: ScalarExpr) -> Option<usize> {
    let ScalarExpr::FunctionCall(call) = predicate else {
        return None;
    };

    let func_name = &call.func_name;
    if func_name == ComparisonOp::Equal.to_func_name() {
        return match (&call.arguments[0], &call.arguments[1]) {
            (ScalarExpr::BoundColumnRef(col), number)
            | (number, ScalarExpr::BoundColumnRef(col))
                if col.column.index == column =>
            {
                extract_i32(number).map(|n| n.max(0) as usize)
            }
            _ => None,
        };
    }

    let (left, right) = match (
        func_name == ComparisonOp::LTE.to_func_name()
            || func_name == ComparisonOp::LT.to_func_name(),
        func_name == ComparisonOp::GTE.to_func_name()
            || func_name == ComparisonOp::GT.to_func_name(),
    ) {
        (true, _) => (&call.arguments[0], &call.arguments[1]),
        (_, true) => (&call.arguments[1], &call.arguments[0]),
        _ => return None,
    };

    let ScalarExpr::BoundColumnRef(col) = left else {
        return None;
    };
    if col.column.index != column {
        return None;
    }

    let eq = func_name == ComparisonOp::GTE.to_func_name()
        || func_name == ComparisonOp::LTE.to_func_name();

    extract_i32(right).map(|n| {
        if eq {
            n.max(0) as usize
        } else {
            n.max(1) as usize - 1
        }
    })
}

fn extract_i32(expr: &ScalarExpr) -> Option<i32> {
    check_number(
        None,
        &FunctionContext::default(),
        &expr.as_expr().ok()?,
        &BUILTIN_FUNCTIONS,
    )
    .ok()
}

fn is_ranking_function(func: &WindowFuncType) -> bool {
    matches!(
        func,
        WindowFuncType::RowNumber | WindowFuncType::Rank | WindowFuncType::DenseRank
    )
}
