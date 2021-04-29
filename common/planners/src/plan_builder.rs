// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use anyhow::bail;
use anyhow::Result;
use common_datavalues::DataField;
use common_datavalues::DataSchema;
use common_datavalues::DataSchemaRef;

use crate::col;
use crate::AggregatorFinalPlan;
use crate::AggregatorPartialPlan;
use crate::EmptyPlan;
use crate::ExplainPlan;
use crate::ExplainType;
use crate::ExpressionPlan;
use crate::FilterPlan;
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

// Case1: group is a column, the name needs to match with aggr
// Case2: aggr is an alias, unfold aggr
// Case3: group and aggr are exactly the same expression
pub fn aggr_group_expr_eq(
    aggr: &ExpressionPlan,
    group: &ExpressionPlan,
    input_schema: &DataSchemaRef
) -> Result<bool> {
    let column_name = aggr.to_data_field(input_schema)?.name().clone();
    if let ExpressionPlan::Column(grp_name) = group {
        if column_name.eq(grp_name) {
            return Ok(true);
        }
    } else if let ExpressionPlan::Alias(_alias, plan) = aggr {
        return aggr_group_expr_eq(plan, group, input_schema);
    } else if String::from(format!("{:?}", group).as_str())
        .eq(&String::from(format!("{:?}", aggr).as_str()))
    {
        return Ok(true);
    }
    return Ok(false);
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

    /// Apply a projection.
    pub fn project(&self, exprs: Vec<ExpressionPlan>) -> Result<Self> {
        let exprs = PlanRewriter::exprs_extract_aliases(exprs)?;
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
            schema: Arc::new(DataSchema::new(fields))
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

        // Aggregator check.
        for e_aggr in &aggr_expr {
            // do not check literal expressions
            if let ExpressionPlan::Literal(_) = e_aggr {
                continue;
            } else if !e_aggr.has_aggregator()? {
                let mut in_group_by = false;
                // Check in e_aggr is in group-by's list
                for e_group in &group_expr {
                    if aggr_group_expr_eq(&e_aggr, &e_group, &input_schema)? {
                        in_group_by = true;
                        break;
                    }
                }
                if !in_group_by {
                    bail!("The expression is not an aggregate function {:?}.", e_aggr);
                }
            }
        }

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
                schema: Arc::new(DataSchema::new(aggr_projection_fields))
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
        let projected_schema = projection
            .clone()
            .map(|p| DataSchema::new(p.iter().map(|i| table_schema.field(*i).clone()).collect()));
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
