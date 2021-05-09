// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_datavalues::DataField;
use common_datavalues::DataSchema;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataSchemaRefExt;
use common_exception::Result;

use crate::col;
use crate::AggregatorFinalPlan;
use crate::AggregatorPartialPlan;
use crate::EmptyPlan;
use crate::ExplainPlan;
use crate::ExplainType;
use crate::ExpressionPlan;
use crate::ExpressionPlan1;
use crate::FilterPlan;
use crate::HavingPlan;
use crate::LimitPlan;
use crate::PlanNode;
use crate::PlanRewriter;
use crate::ProjectionPlan;
use crate::ScanPlan;
use crate::SelectPlan;
use crate::SortPlan;
use crate::StagePlan;
use crate::StageState;

pub enum AggregateMode {
    Partial,
    Final
}

pub struct PlanBuilder {
    plan: PlanNode
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
            schema: DataSchemaRef::new(DataSchema::empty())
        }))
    }

    pub fn exprs_to_fields(
        &self,
        exprs: &[ExpressionPlan],
        input_schema: &DataSchemaRef
    ) -> Result<Vec<DataField>> {
        exprs
            .iter()
            .map(|expr| expr.to_data_field(input_schema))
            .collect::<Result<_>>()
    }

    /// Apply a stage.
    pub fn stage(&self, uuid: String, state: StageState) -> Result<Self> {
        Ok(Self::from(&PlanNode::Stage(StagePlan {
            uuid,
            id: 0,
            state,
            input: Arc::new(self.plan.clone())
        })))
    }

    /// Apply a expression and merge the fields with exprs.
    pub fn expression(&self, exprs: &[ExpressionPlan], desc: &str) -> Result<Self> {
        let input_schema = self.plan.schema();

        // Merge fields.
        let mut merged = input_schema.fields().clone();
        let fields = self.exprs_to_fields(&exprs, &input_schema)?;
        for field in fields {
            if !merged.iter().any(|x| x.name() == field.name()) {
                merged.push(field);
            }
        }

        Ok(Self::from(&PlanNode::Expression(ExpressionPlan1 {
            input: Arc::new(self.plan.clone()),
            exprs: Vec::from(exprs),
            schema: DataSchemaRefExt::create(merged),
            desc: desc.to_string()
        })))
    }

    /// Apply a projection.
    pub fn project(&self, exprs: Vec<ExpressionPlan>) -> Result<Self> {
        let exprs = PlanRewriter::rewrite_projection_aliases(exprs)?;
        let input_schema = self.plan.schema();

        let mut projection_exprs = vec![];
        exprs.iter().for_each(|v| match v {
            ExpressionPlan::Wildcard => {
                for i in 0..input_schema.fields().len() {
                    projection_exprs.push(col(input_schema.fields()[i].name()))
                }
            }
            _ => projection_exprs.push(v.clone())
        });

        let fields = self.exprs_to_fields(&projection_exprs, &input_schema)?;
        Ok(Self::from(&PlanNode::Projection(ProjectionPlan {
            input: Arc::new(self.plan.clone()),
            expr: projection_exprs,
            schema: DataSchemaRefExt::create(fields)
        })))
    }

    fn aggregate(
        &self,
        mode: AggregateMode,
        aggr_expr: Vec<ExpressionPlan>,
        group_expr: Vec<ExpressionPlan>
    ) -> Result<Self> {
        let input_schema = self.plan.schema();
        let aggr_projection_fields = self.exprs_to_fields(&aggr_expr, &input_schema)?;

        Ok(match mode {
            AggregateMode::Partial => {
                Self::from(&PlanNode::AggregatorPartial(AggregatorPartialPlan {
                    input: Arc::new(self.plan.clone()),
                    aggr_expr,
                    group_expr
                }))
            }
            AggregateMode::Final => Self::from(&PlanNode::AggregatorFinal(AggregatorFinalPlan {
                input: Arc::new(self.plan.clone()),
                aggr_expr,
                group_expr,
                schema: DataSchemaRefExt::create(aggr_projection_fields)
            }))
        })
    }

    /// Apply a partial aggregator plan.
    pub fn aggregate_partial(
        &self,
        aggr_expr: Vec<ExpressionPlan>,
        group_expr: Vec<ExpressionPlan>
    ) -> Result<Self> {
        self.aggregate(AggregateMode::Partial, aggr_expr, group_expr)
    }

    /// Apply a final aggregator plan.
    pub fn aggregate_final(
        &self,
        aggr_expr: Vec<ExpressionPlan>,
        group_expr: Vec<ExpressionPlan>
    ) -> Result<Self> {
        self.aggregate(AggregateMode::Final, aggr_expr, group_expr)
    }

    /// Scan a data source
    pub fn scan(
        schema_name: &str,
        _table_name: &str,
        table_schema: &DataSchema,
        projection: Option<Vec<usize>>,
        table_args: Option<ExpressionPlan>,
        limit: Option<usize>
    ) -> Result<Self> {
        let table_schema = DataSchemaRef::new(table_schema.clone());
        let projected_schema = projection.clone().map(|p| {
            DataSchemaRefExt::create(p.iter().map(|i| table_schema.field(*i).clone()).collect())
                .as_ref()
                .clone()
        });
        let projected_schema = match projected_schema {
            None => table_schema.clone(),
            Some(v) => Arc::new(v)
        };

        Ok(Self::from(&PlanNode::Scan(ScanPlan {
            schema_name: schema_name.to_owned(),
            table_schema,
            projected_schema,
            projection,
            table_args,
            filters: vec![],
            limit
        })))
    }

    /// Apply a filter
    pub fn filter(&self, expr: ExpressionPlan) -> Result<Self> {
        Ok(Self::from(&PlanNode::Filter(FilterPlan {
            predicate: expr,
            input: Arc::new(self.plan.clone())
        })))
    }

    /// Apply a having
    pub fn having(&self, expr: ExpressionPlan) -> Result<Self> {
        Ok(Self::from(&PlanNode::Having(HavingPlan {
            predicate: expr,
            input: Arc::new(self.plan.clone())
        })))
    }

    pub fn sort(&self, exprs: &[ExpressionPlan]) -> Result<Self> {
        Ok(Self::from(&PlanNode::Sort(SortPlan {
            order_by: exprs.to_vec(),
            input: Arc::new(self.plan.clone())
        })))
    }

    /// Apply a limit
    pub fn limit(&self, n: usize) -> Result<Self> {
        Ok(Self::from(&PlanNode::Limit(LimitPlan {
            n,
            input: Arc::new(self.plan.clone())
        })))
    }

    pub fn select(&self) -> Result<Self> {
        Ok(Self::from(&PlanNode::Select(SelectPlan {
            input: Arc::new(self.plan.clone())
        })))
    }

    pub fn explain(&self) -> Result<Self> {
        Ok(Self::from(&PlanNode::Explain(ExplainPlan {
            typ: ExplainType::Syntax,
            input: Arc::new(self.plan.clone())
        })))
    }

    /// Build the plan
    pub fn build(&self) -> Result<PlanNode> {
        Ok(self.plan.clone())
    }
}
