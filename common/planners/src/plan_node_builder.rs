// Copyright 2021 Datafuse Labs.
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

use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::Result;

use crate::col;
use crate::plan_subqueries_set::SubQueriesSetPlan;
use crate::validate_expression;
use crate::AggregatorFinalPlan;
use crate::AggregatorPartialPlan;
use crate::EmptyPlan;
use crate::ExplainPlan;
use crate::ExplainType;
use crate::Expression;
use crate::ExpressionPlan;
use crate::FilterPlan;
use crate::HavingPlan;
use crate::LimitByPlan;
use crate::LimitPlan;
use crate::PlanNode;
use crate::ProjectionPlan;
use crate::RewriteHelper;
use crate::SelectPlan;
use crate::SortPlan;

pub enum AggregateMode {
    Partial,
    Final,
}

pub struct PlanBuilder {
    plan: PlanNode,
}

impl PlanBuilder {
    /// Create a builder from an existing plan
    pub fn from(plan: &PlanNode) -> Self {
        Self { plan: plan.clone() }
    }

    pub fn create(schema: DataSchemaRef) -> Self {
        Self::from(&PlanNode::Empty(EmptyPlan::create_with_schema(schema)))
    }

    /// Create an empty relation.
    pub fn empty() -> Self {
        Self::from(&PlanNode::Empty(EmptyPlan::create_with_schema(
            DataSchemaRef::new(DataSchema::empty()),
        )))
    }

    /// Apply a expression and merge the fields with exprs.
    pub fn expression(&self, exprs: &[Expression], desc: &str) -> Result<Self> {
        let input_schema = self.plan.schema();

        // Get the projection expressions(Including rewrite).
        let mut projection_exprs = vec![];
        exprs.iter().for_each(|v| match v {
            Expression::Wildcard => {
                for i in 0..input_schema.fields().len() {
                    projection_exprs.push(col(input_schema.fields()[i].name()))
                }
            }
            _ => projection_exprs.push(v.clone()),
        });

        // Let's validate the expressions firstly
        for expr in projection_exprs.iter() {
            validate_expression(expr)?;
        }

        // Merge fields.
        let fields = RewriteHelper::exprs_to_fields(&projection_exprs, &input_schema)?;
        let mut merged = input_schema.fields().clone();
        for field in fields {
            if !merged.iter().any(|x| x.name() == field.name()) && field.name() != "*" {
                merged.push(field);
            }
        }

        Ok(PlanBuilder::from(&PlanNode::Expression(ExpressionPlan {
            input: self.wrap_subquery_plan(&projection_exprs)?,
            exprs: projection_exprs.clone(),
            schema: DataSchemaRefExt::create(merged),
            desc: desc.to_string(),
        })))
    }

    /// Apply a projection.
    pub fn project(&self, exprs: &[Expression]) -> Result<Self> {
        let input_schema = self.plan.schema();
        let fields = RewriteHelper::exprs_to_fields(exprs, &input_schema)?;

        Ok(Self::from(&PlanNode::Projection(ProjectionPlan {
            input: self.wrap_subquery_plan(exprs)?,
            expr: exprs.to_owned(),
            schema: DataSchemaRefExt::create(fields),
        })))
    }

    fn aggregate(
        &self,
        mode: AggregateMode,
        schema_before_groupby: DataSchemaRef,
        aggr_expr: &[Expression],
        group_expr: &[Expression],
    ) -> Result<Self> {
        Ok(match mode {
            AggregateMode::Partial => {
                let fields = RewriteHelper::exprs_to_fields(aggr_expr, &schema_before_groupby)?;

                let mut partial_fields = fields
                    .iter()
                    .map(|f| DataField::new(f.name(), Vu8::to_data_type()))
                    .collect::<Vec<_>>();

                if !group_expr.is_empty() {
                    // Fields. [aggrs,  key]
                    // aggrs: aggr_len aggregate states
                    // key: Varint by hash method

                    let group_cols: Vec<String> =
                        group_expr.iter().map(|expr| expr.column_name()).collect();
                    let sample_block = DataBlock::empty_with_schema(schema_before_groupby);
                    let method = DataBlock::choose_hash_method(&sample_block, &group_cols)?;
                    partial_fields.push(DataField::new("_group_by_key", method.data_type()));
                }

                Self::from(&PlanNode::AggregatorPartial(AggregatorPartialPlan {
                    input: Arc::new(self.plan.clone()),
                    aggr_expr: aggr_expr.to_vec(),
                    group_expr: group_expr.to_vec(),
                    schema: DataSchemaRefExt::create(partial_fields),
                }))
            }
            AggregateMode::Final => {
                let mut final_exprs = aggr_expr.to_owned();
                final_exprs.extend_from_slice(group_expr);
                let final_fields =
                    RewriteHelper::exprs_to_fields(&final_exprs, &schema_before_groupby)?;

                Self::from(&PlanNode::AggregatorFinal(AggregatorFinalPlan {
                    input: Arc::new(self.plan.clone()),
                    aggr_expr: aggr_expr.to_vec(),
                    group_expr: group_expr.to_vec(),
                    schema: DataSchemaRefExt::create(final_fields),
                    schema_before_group_by: schema_before_groupby,
                }))
            }
        })
    }

    /// Apply a partial aggregator plan.
    pub fn aggregate_partial(
        &self,
        aggr_expr: &[Expression],
        group_expr: &[Expression],
    ) -> Result<Self> {
        self.aggregate(
            AggregateMode::Partial,
            self.plan.schema(),
            aggr_expr,
            group_expr,
        )
    }

    /// Apply a final aggregator plan.
    pub fn aggregate_final(
        &self,
        schema_before_group_by: DataSchemaRef,
        aggr_expr: &[Expression],
        group_expr: &[Expression],
    ) -> Result<Self> {
        self.aggregate(
            AggregateMode::Final,
            schema_before_group_by,
            aggr_expr,
            group_expr,
        )
    }

    /// Apply a filter
    pub fn filter(&self, expr: Expression) -> Result<Self> {
        validate_expression(&expr)?;
        Ok(Self::from(&PlanNode::Filter(FilterPlan {
            predicate: expr.clone(),
            schema: self.plan.schema(),
            input: self.wrap_subquery_plan(&[expr])?,
        })))
    }

    /// Apply a having
    pub fn having(&self, expr: Expression) -> Result<Self> {
        validate_expression(&expr)?;
        Ok(Self::from(&PlanNode::Having(HavingPlan {
            predicate: expr.clone(),
            schema: self.plan.schema(),
            input: self.wrap_subquery_plan(&[expr])?,
        })))
    }

    pub fn sort(&self, exprs: &[Expression]) -> Result<Self> {
        Ok(Self::from(&PlanNode::Sort(SortPlan {
            order_by: exprs.to_vec(),
            schema: self.plan.schema(),
            input: self.wrap_subquery_plan(exprs)?,
        })))
    }

    /// Apply a limit
    pub fn limit(&self, n: usize) -> Result<Self> {
        Ok(Self::from(&PlanNode::Limit(LimitPlan {
            n: Some(n),
            offset: 0,
            input: Arc::new(self.plan.clone()),
        })))
    }

    /// Apply a limit offset
    pub fn limit_offset(&self, n: Option<usize>, offset: usize) -> Result<Self> {
        Ok(Self::from(&PlanNode::Limit(LimitPlan {
            n,
            offset,
            input: Arc::new(self.plan.clone()),
        })))
    }

    pub fn limit_by(&self, n: usize, exprs: &[Expression]) -> Result<Self> {
        Ok(Self::from(&PlanNode::LimitBy(LimitByPlan {
            limit: n,
            input: Arc::new(self.plan.clone()),
            limit_by: exprs.to_vec(),
        })))
    }

    pub fn select(&self) -> Result<Self> {
        Ok(Self::from(&PlanNode::Select(SelectPlan {
            input: Arc::new(self.plan.clone()),
        })))
    }

    pub fn explain(&self) -> Result<Self> {
        Ok(Self::from(&PlanNode::Explain(ExplainPlan {
            typ: ExplainType::Syntax,
            input: Arc::new(self.plan.clone()),
        })))
    }

    /// Build the plan
    pub fn build(&self) -> Result<PlanNode> {
        Ok(self.plan.clone())
    }

    fn wrap_subquery_plan(&self, exprs: &[Expression]) -> Result<Arc<PlanNode>> {
        let input = &self.plan;
        let sub_queries = RewriteHelper::collect_exprs_sub_queries(exprs)?;
        match sub_queries.is_empty() {
            true => Ok(Arc::new(input.clone())),
            false => Ok(Arc::new(PlanNode::SubQueryExpression(SubQueriesSetPlan {
                expressions: sub_queries,
                input: Arc::new(input.clone()),
            }))),
        }
    }
}
