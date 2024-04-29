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

use crate::binder::split_conjunctions;
use crate::optimizer::filter::InferFilterOptimizer;
use crate::optimizer::filter::NormalizeDisjunctiveFilterOptimizer;
use crate::optimizer::SExpr;
use crate::plans::EvalScalar;
use crate::plans::Filter;
use crate::plans::FunctionCall;
use crate::plans::Join;
use crate::plans::JoinType;
use crate::plans::RelOperator;
use crate::plans::ScalarItem;
use crate::plans::WindowFuncType;
use crate::MetadataRef;
use crate::ScalarExpr;

// The PullUpFilterOptimizer will pull up filters to the top of the plan tree and infer new filters.
pub struct PullUpFilterOptimizer {
    pub predicates: Vec<ScalarExpr>,
    pub metadata: MetadataRef,
}

impl PullUpFilterOptimizer {
    pub fn new(metadata: MetadataRef) -> Self {
        PullUpFilterOptimizer {
            predicates: vec![],
            metadata,
        }
    }

    pub fn run(mut self, s_expr: &SExpr) -> Result<SExpr> {
        let mut s_expr = self.pull_up(s_expr)?;
        s_expr = self.finish(s_expr)?;
        Ok(s_expr)
    }

    pub fn finish(self, s_expr: SExpr) -> Result<SExpr> {
        if self.predicates.is_empty() {
            Ok(s_expr)
        } else {
            let predicates = InferFilterOptimizer::new(None).run(self.predicates)?;
            let predicates = NormalizeDisjunctiveFilterOptimizer::new().run(predicates)?;
            let filter = Filter { predicates };
            Ok(SExpr::create_unary(
                Arc::new(filter.into()),
                Arc::new(s_expr),
            ))
        }
    }

    pub fn pull_up(&mut self, s_expr: &SExpr) -> Result<SExpr> {
        match s_expr.plan.as_ref() {
            RelOperator::Filter(filter) => self.pull_up_filter(s_expr, filter),
            RelOperator::Join(join) if !join.is_lateral => self.pull_up_join(s_expr, join),
            RelOperator::EvalScalar(eval_scalar) => self.pull_up_eval_scalar(s_expr, eval_scalar),
            RelOperator::MaterializedCte(_) => Ok(s_expr.clone()),
            _ => self.pull_up_others(s_expr),
        }
    }

    fn pull_up_filter(&mut self, s_expr: &SExpr, filter: &Filter) -> Result<SExpr> {
        let child = self.pull_up(s_expr.child(0)?)?;
        for predicate in filter.predicates.iter() {
            self.predicates.extend(split_conjunctions(predicate));
        }
        Ok(child)
    }

    fn pull_up_join(&mut self, s_expr: &SExpr, join: &Join) -> Result<SExpr> {
        let (left_need_pull_up, right_need_pull_up) = match join.join_type {
            JoinType::Inner | JoinType::Cross => (true, true),
            JoinType::Left | JoinType::LeftSingle | JoinType::LeftSemi | JoinType::LeftAnti => {
                (true, false)
            }
            JoinType::Right | JoinType::RightSingle | JoinType::RightSemi | JoinType::RightAnti => {
                (false, true)
            }
            _ => (false, false),
        };
        let mut left_pull_up = PullUpFilterOptimizer::new(self.metadata.clone());
        let mut right_pull_up = PullUpFilterOptimizer::new(self.metadata.clone());
        let mut left = left_pull_up.pull_up(s_expr.child(0)?)?;
        let mut right = right_pull_up.pull_up(s_expr.child(1)?)?;
        if left_need_pull_up {
            for predicate in left_pull_up.predicates {
                self.predicates.extend(split_conjunctions(&predicate));
            }
        } else {
            left = left_pull_up.finish(left)?;
        }
        if right_need_pull_up {
            for predicate in right_pull_up.predicates {
                self.predicates.extend(split_conjunctions(&predicate));
            }
        } else {
            right = right_pull_up.finish(right)?;
        }
        let mut join = join.clone();
        if left_need_pull_up && right_need_pull_up {
            for (left_condition, right_condition) in join
                .left_conditions
                .iter()
                .zip(join.right_conditions.iter())
            {
                let predicate = ScalarExpr::FunctionCall(FunctionCall {
                    span: None,
                    func_name: "eq".to_string(),
                    params: vec![],
                    arguments: vec![left_condition.clone(), right_condition.clone()],
                });
                self.predicates.push(predicate);
            }
            for predicate in join.non_equi_conditions.iter() {
                self.predicates.extend(split_conjunctions(predicate));
            }
            join.left_conditions.clear();
            join.right_conditions.clear();
            join.non_equi_conditions.clear();
            join.join_type = JoinType::Cross;
        }
        let s_expr = s_expr.replace_plan(Arc::new(RelOperator::Join(join)));
        Ok(s_expr.replace_children(vec![Arc::new(left), Arc::new(right)]))
    }

    fn pull_up_eval_scalar(&mut self, s_expr: &SExpr, eval_scalar: &EvalScalar) -> Result<SExpr> {
        let child = self.pull_up(s_expr.child(0)?)?;
        let mut eval_scalar = eval_scalar.clone();
        for predicate in self.predicates.iter_mut() {
            Self::replace_predicate(predicate, &mut eval_scalar.items, &self.metadata)?;
        }
        let s_expr = s_expr.replace_plan(Arc::new(RelOperator::EvalScalar(eval_scalar)));
        Ok(s_expr.replace_children(vec![Arc::new(child)]))
    }

    pub fn pull_up_others(&mut self, s_expr: &SExpr) -> Result<SExpr> {
        let mut children = Vec::with_capacity(s_expr.arity());
        for child in s_expr.children() {
            let child = PullUpFilterOptimizer::new(self.metadata.clone()).run(child)?;
            children.push(Arc::new(child));
        }
        Ok(s_expr.replace_children(children))
    }

    fn replace_predicate(
        predicate: &mut ScalarExpr,
        items: &mut Vec<ScalarItem>,
        metadata: &MetadataRef,
    ) -> Result<()> {
        match predicate {
            ScalarExpr::BoundColumnRef(column) => {
                for item in items.iter() {
                    if item.index == column.column.index {
                        return Ok(());
                    }
                    if let ScalarExpr::BoundColumnRef(item_column) = &item.scalar {
                        if item_column.column.index == column.column.index {
                            column.column.index = item.index;
                            return Ok(());
                        }
                    }
                }

                let new_index = metadata.write().add_derived_column(
                    column.column.column_name.clone(),
                    *column.column.data_type.clone(),
                );
                let new_column = column.clone();
                items.push(ScalarItem {
                    scalar: ScalarExpr::BoundColumnRef(new_column),
                    index: new_index,
                });
                column.column.index = new_index;
            }
            ScalarExpr::WindowFunction(window) => {
                match &mut window.func {
                    WindowFuncType::Aggregate(agg) => {
                        for arg in agg.args.iter_mut() {
                            Self::replace_predicate(arg, items, metadata)?;
                        }
                    }
                    WindowFuncType::LagLead(ll) => {
                        Self::replace_predicate(&mut ll.arg, items, metadata)?;
                        if let Some(default) = ll.default.as_mut() {
                            Self::replace_predicate(default, items, metadata)?;
                        }
                    }
                    WindowFuncType::NthValue(func) => {
                        Self::replace_predicate(&mut func.arg, items, metadata)?;
                    }
                    _ => (),
                };

                for window_partition_by in window.partition_by.iter_mut() {
                    Self::replace_predicate(window_partition_by, items, metadata)?;
                }

                for window_order_by in window.order_by.iter_mut() {
                    Self::replace_predicate(&mut window_order_by.expr, items, metadata)?;
                }
            }
            ScalarExpr::AggregateFunction(agg_func) => {
                for arg in agg_func.args.iter_mut() {
                    Self::replace_predicate(arg, items, metadata)?;
                }
            }
            ScalarExpr::FunctionCall(func) => {
                for arg in func.arguments.iter_mut() {
                    Self::replace_predicate(arg, items, metadata)?;
                }
            }
            ScalarExpr::LambdaFunction(lambda_func) => {
                for arg in lambda_func.args.iter_mut() {
                    Self::replace_predicate(arg, items, metadata)?;
                }
            }
            ScalarExpr::CastExpr(cast) => {
                Self::replace_predicate(&mut cast.argument, items, metadata)?;
            }
            ScalarExpr::UDFCall(udf) => {
                for arg in udf.arguments.iter_mut() {
                    Self::replace_predicate(arg, items, metadata)?;
                }
            }
            ScalarExpr::UDFLambdaCall(udf) => {
                Self::replace_predicate(&mut udf.scalar, items, metadata)?;
            }
            _ => (),
        }
        Ok(())
    }
}
