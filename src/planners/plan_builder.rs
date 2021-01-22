// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::sync::Arc;

use crate::datavalues::{DataField, DataSchema, DataSchemaRef};
use crate::error::FuseQueryResult;
use crate::planners::{
    field, AggregatePlan, DFExplainType, EmptyPlan, ExplainPlan, ExpressionPlan, FilterPlan,
    LimitPlan, PlanNode, ProjectionPlan, ScanPlan, SelectPlan,
};

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

    /// Apply a projection.
    pub fn project(&self, exprs: Vec<ExpressionPlan>) -> FuseQueryResult<Self> {
        let input_schema = self.plan.schema();

        let mut projection_exprs = vec![];
        exprs.iter().for_each(|v| match v {
            ExpressionPlan::Wildcard => {
                for i in 0..input_schema.fields().len() {
                    projection_exprs.push(field(input_schema.fields()[i].name()))
                }
            }
            _ => projection_exprs.push(v.clone()),
        });
        let fields: Vec<DataField> = projection_exprs
            .iter()
            .map(|expr| expr.to_field(&input_schema))
            .collect::<FuseQueryResult<_>>()?;

        Ok(Self::from(&PlanNode::Projection(ProjectionPlan {
            input: Arc::new(self.plan.clone()),
            expr: projection_exprs,
            schema: Arc::new(DataSchema::new(fields)),
        })))
    }

    /// Apply an aggregate
    pub fn aggregate(
        &self,
        group_expr: Vec<ExpressionPlan>,
        aggr_expr: Vec<ExpressionPlan>,
    ) -> FuseQueryResult<Self> {
        let mut all_expr: Vec<ExpressionPlan> = group_expr.clone();
        aggr_expr.iter().for_each(|x| all_expr.push(x.clone()));

        let input_schema = self.plan.schema();
        let aggr_fields: Vec<DataField> = all_expr
            .iter()
            .map(|expr| expr.to_field(&input_schema))
            .collect::<FuseQueryResult<_>>()?;

        Ok(Self::from(&PlanNode::Aggregate(AggregatePlan {
            input: Arc::new(self.plan.clone()),
            group_expr,
            aggr_expr,
            schema: Arc::new(DataSchema::new(aggr_fields)),
        })))
    }

    /// Scan a data source
    pub fn scan(
        schema_name: &str,
        _table_name: &str,
        table_schema: &DataSchema,
        projection: Option<Vec<usize>>,
        table_args: Option<ExpressionPlan>,
    ) -> FuseQueryResult<Self> {
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
    pub fn filter(&self, expr: ExpressionPlan) -> FuseQueryResult<Self> {
        Ok(Self::from(&PlanNode::Filter(FilterPlan {
            predicate: expr,
            input: Arc::new(self.plan.clone()),
        })))
    }

    /// Apply a limit
    pub fn limit(&self, n: usize) -> FuseQueryResult<Self> {
        Ok(Self::from(&PlanNode::Limit(LimitPlan {
            n,
            input: Arc::new(self.plan.clone()),
        })))
    }

    pub fn select(&self) -> FuseQueryResult<Self> {
        Ok(Self::from(&PlanNode::Select(SelectPlan {
            plan: Box::new(self.plan.clone()),
        })))
    }

    pub fn explain(&self) -> FuseQueryResult<Self> {
        Ok(Self::from(&PlanNode::Explain(ExplainPlan {
            typ: DFExplainType::Syntax,
            plan: Box::new(self.plan.clone()),
        })))
    }

    /// Build the plan
    pub fn build(&self) -> FuseQueryResult<PlanNode> {
        Ok(self.plan.clone())
    }
}
