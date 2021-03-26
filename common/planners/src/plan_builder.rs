// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use crate::datavalues::{DataField, DataSchema, DataSchemaRef};
use crate::PlannerResult;
use crate::{
    col, AggregatorFinalPlan, AggregatorPartialPlan, DfExplainType, EmptyPlan, ExplainPlan,
    ExpressionPlan, FilterPlan, LimitPlan, PlanNode, PlanRewriter, ProjectionPlan, ScanPlan,
    SelectPlan, StagePlan, StageState,
};

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
        Self::from(&PlanNode::Empty(EmptyPlan { schema }))
    }

    /// Create an empty relation.
    pub fn empty() -> Self {
        Self::from(&PlanNode::Empty(EmptyPlan {
            schema: DataSchemaRef::new(DataSchema::empty()),
        }))
    }

    pub fn exprs_to_fields(
        &self,
        exprs: &[ExpressionPlan],
        input_schema: &DataSchemaRef,
    ) -> PlannerResult<Vec<DataField>> {
        exprs
            .iter()
            .map(|expr| expr.to_data_field(input_schema))
            .collect::<PlannerResult<_>>()
    }

    /// Apply a stage.
    pub fn stage(&self, uuid: String, state: StageState) -> PlannerResult<Self> {
        Ok(Self::from(&PlanNode::Stage(StagePlan {
            uuid,
            id: 0,
            state,
            input: Arc::new(self.plan.clone()),
        })))
    }

    /// Apply a projection.
    pub fn project(&self, exprs: Vec<ExpressionPlan>) -> PlannerResult<Self> {
        let exprs = PlanRewriter::exprs_extract_aliases(exprs)?;
        let input_schema = self.plan.schema();

        let mut projection_exprs = vec![];
        exprs.iter().for_each(|v| match v {
            ExpressionPlan::Wildcard => {
                for i in 0..input_schema.fields().len() {
                    projection_exprs.push(col(input_schema.fields()[i].name()))
                }
            }
            _ => projection_exprs.push(v.clone()),
        });

        let fields = self.exprs_to_fields(&projection_exprs, &input_schema)?;
        Ok(Self::from(&PlanNode::Projection(ProjectionPlan {
            input: Arc::new(self.plan.clone()),
            expr: projection_exprs,
            schema: Arc::new(DataSchema::new(fields)),
        })))
    }

    fn aggregate(
        &self,
        mode: AggregateMode,
        aggr_expr: Vec<ExpressionPlan>,
        group_expr: Vec<ExpressionPlan>,
    ) -> PlannerResult<Self> {
        let mut all_expr: Vec<ExpressionPlan> = group_expr.clone();
        aggr_expr.iter().for_each(|x| all_expr.push(x.clone()));

        let input_schema = self.plan.schema();
        let aggr_fields = self.exprs_to_fields(&all_expr, &input_schema)?;

        Ok(match mode {
            AggregateMode::Partial => {
                Self::from(&PlanNode::AggregatorPartial(AggregatorPartialPlan {
                    input: Arc::new(self.plan.clone()),
                    aggr_expr,
                    group_expr,
                }))
            }
            AggregateMode::Final => Self::from(&PlanNode::AggregatorFinal(AggregatorFinalPlan {
                input: Arc::new(self.plan.clone()),
                aggr_expr,
                group_expr,
                schema: Arc::new(DataSchema::new(aggr_fields)),
            })),
        })
    }

    /// Apply a partial aggregator plan.
    pub fn aggregate_partial(
        &self,
        aggr_expr: Vec<ExpressionPlan>,
        group_expr: Vec<ExpressionPlan>,
    ) -> PlannerResult<Self> {
        self.aggregate(AggregateMode::Partial, aggr_expr, group_expr)
    }

    /// Apply a final aggregator plan.
    pub fn aggregate_final(
        &self,
        aggr_expr: Vec<ExpressionPlan>,
        group_expr: Vec<ExpressionPlan>,
    ) -> PlannerResult<Self> {
        self.aggregate(AggregateMode::Final, aggr_expr, group_expr)
    }

    /// Scan a data source
    pub fn scan(
        schema_name: &str,
        _table_name: &str,
        table_schema: &DataSchema,
        projection: Option<Vec<usize>>,
        table_args: Option<ExpressionPlan>,
    ) -> PlannerResult<Self> {
        let table_schema = DataSchemaRef::new(table_schema.clone());
        let projected_schema = projection
            .clone()
            .map(|p| DataSchema::new(p.iter().map(|i| table_schema.field(*i).clone()).collect()));
        let projected_schema = match projected_schema {
            None => table_schema.clone(),
            Some(v) => Arc::new(v),
        };

        Ok(Self::from(&PlanNode::Scan(ScanPlan {
            schema_name: schema_name.to_owned(),
            table_schema,
            projected_schema,
            projection,
            table_args,
        })))
    }

    /// Apply a filter
    pub fn filter(&self, expr: ExpressionPlan) -> PlannerResult<Self> {
        Ok(Self::from(&PlanNode::Filter(FilterPlan {
            predicate: expr,
            input: Arc::new(self.plan.clone()),
        })))
    }

    /// Apply a limit
    pub fn limit(&self, n: usize) -> PlannerResult<Self> {
        Ok(Self::from(&PlanNode::Limit(LimitPlan {
            n,
            input: Arc::new(self.plan.clone()),
        })))
    }

    pub fn select(&self) -> PlannerResult<Self> {
        Ok(Self::from(&PlanNode::Select(SelectPlan {
            input: Arc::new(self.plan.clone()),
        })))
    }

    pub fn explain(&self) -> PlannerResult<Self> {
        Ok(Self::from(&PlanNode::Explain(ExplainPlan {
            typ: DfExplainType::Syntax,
            input: Arc::new(self.plan.clone()),
        })))
    }

    /// Build the plan
    pub fn build(&self) -> PlannerResult<PlanNode> {
        Ok(self.plan.clone())
    }
}
