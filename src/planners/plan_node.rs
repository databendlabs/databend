// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use crate::contexts::FuseQueryContextRef;
use crate::datavalues::DataSchemaRef;
use crate::error::{FuseQueryError, FuseQueryResult};
use crate::planners::{
    AggregatorFinalPlan, AggregatorPartialPlan, EmptyPlan, ExplainPlan, FilterPlan, FragmentPlan,
    LimitPlan, PlanBuilder, ProjectionPlan, ReadDataSourcePlan, ScanPlan, SelectPlan, SettingPlan,
};

#[derive(Clone)]
pub enum PlanNode {
    Empty(EmptyPlan),
    Fragment(FragmentPlan),
    Projection(ProjectionPlan),
    AggregatorPartial(AggregatorPartialPlan),
    AggregatorFinal(AggregatorFinalPlan),
    Filter(FilterPlan),
    Limit(LimitPlan),
    Scan(ScanPlan),
    ReadSource(ReadDataSourcePlan),
    Explain(ExplainPlan),
    Select(SelectPlan),
    SetVariable(SettingPlan),
}

impl PlanNode {
    /// Get a reference to the logical plan's schema
    pub fn schema(&self) -> DataSchemaRef {
        match self {
            PlanNode::Empty(v) => v.schema(),
            PlanNode::Fragment(v) => v.schema(),
            PlanNode::Scan(v) => v.schema(),
            PlanNode::Projection(v) => v.schema(),
            PlanNode::AggregatorPartial(v) => v.schema(),
            PlanNode::AggregatorFinal(v) => v.schema(),
            PlanNode::Filter(v) => v.schema(),
            PlanNode::Limit(v) => v.schema(),
            PlanNode::ReadSource(v) => v.schema(),
            PlanNode::Select(v) => v.plan.schema(),
            PlanNode::Explain(_) => unimplemented!(),
            PlanNode::SetVariable(_) => unimplemented!(),
        }
    }

    pub fn name(&self) -> &str {
        match self {
            PlanNode::Empty(_) => "EmptyPlan",
            PlanNode::Fragment(_) => "FragmentPlan",
            PlanNode::Scan(_) => "ScanPlan",
            PlanNode::Projection(_) => "ProjectionPlan",
            PlanNode::AggregatorPartial(_) => "AggregatorPartialPlan",
            PlanNode::AggregatorFinal(_) => "AggregatorFinalPlan",
            PlanNode::Filter(_) => "FilterPlan",
            PlanNode::Limit(_) => "LimitPlan",
            PlanNode::ReadSource(_) => "ReadSourcePlan",
            PlanNode::Select(_) => "SelectPlan",
            PlanNode::Explain(_) => "ExplainPlan",
            PlanNode::SetVariable(_) => "SetVariablePlan",
        }
    }

    /// build plan node to list
    /// with_parent only affected select/explain
    fn build_plan_list(&self, with_parent: bool) -> FuseQueryResult<Vec<PlanNode>> {
        let max_depth = 128;
        let mut depth = 0;
        let mut list = vec![];
        let mut plan = self.clone();

        loop {
            if depth > max_depth {
                return Err(FuseQueryError::Plan(format!(
                    "PlanNode depth more than {}",
                    max_depth
                )));
            }

            match plan {
                PlanNode::Fragment(v) => {
                    list.push(PlanNode::Fragment(v.clone()));
                    plan = v.input.as_ref().clone();
                    depth += 1;
                }
                PlanNode::Projection(v) => {
                    list.push(PlanNode::Projection(v.clone()));
                    plan = v.input.as_ref().clone();
                    depth += 1;
                }
                PlanNode::AggregatorPartial(v) => {
                    list.push(PlanNode::AggregatorPartial(v.clone()));
                    plan = v.input.as_ref().clone();
                    depth += 1;
                }
                PlanNode::AggregatorFinal(v) => {
                    list.push(PlanNode::AggregatorFinal(v.clone()));
                    plan = v.input.as_ref().clone();
                    depth += 1;
                }
                PlanNode::Filter(v) => {
                    list.push(PlanNode::Filter(v.clone()));
                    plan = v.input.as_ref().clone();
                    depth += 1;
                }
                PlanNode::Limit(v) => {
                    list.push(PlanNode::Limit(v.clone()));
                    plan = v.input.as_ref().clone();
                    depth += 1;
                }
                PlanNode::Select(v) => {
                    if with_parent {
                        list.push(PlanNode::Select(v.clone()));
                    }
                    plan = v.plan.as_ref().clone();
                    depth += 1;
                }
                PlanNode::Explain(v) => {
                    if with_parent {
                        list.push(PlanNode::Explain(v.clone()));
                    }
                    plan = v.plan.as_ref().clone();
                    depth += 1;
                }

                // Return.
                PlanNode::Empty(_) => {
                    break;
                }
                PlanNode::Scan(v) => {
                    list.push(PlanNode::Scan(v));
                    break;
                }
                PlanNode::ReadSource(v) => {
                    list.push(PlanNode::ReadSource(v));
                    break;
                }
                PlanNode::SetVariable(_) => {
                    break;
                }
            }
        }
        list.reverse();
        Ok(list)
    }

    pub fn get_children_nodes(&self) -> FuseQueryResult<Vec<PlanNode>> {
        self.build_plan_list(false)
    }

    pub fn get_all_nodes(&self) -> FuseQueryResult<Vec<PlanNode>> {
        self.build_plan_list(true)
    }

    pub fn plan_list_to_node(
        ctx: FuseQueryContextRef,
        list: &[PlanNode],
    ) -> FuseQueryResult<PlanNode> {
        let mut builder = PlanBuilder::empty(ctx.clone());
        for plan in list {
            match plan {
                PlanNode::Projection(v) => {
                    builder = builder.project(v.expr.clone())?;
                }
                PlanNode::AggregatorPartial(v) => {
                    builder =
                        builder.aggregate_partial(v.aggr_expr.clone(), v.group_expr.clone())?;
                }
                PlanNode::AggregatorFinal(v) => {
                    builder = builder.aggregate_final(v.aggr_expr.clone(), v.group_expr.clone())?;
                }
                PlanNode::Filter(v) => {
                    builder = builder.filter(v.predicate.clone())?;
                }
                PlanNode::Limit(v) => {
                    builder = builder.limit(v.n)?;
                }
                PlanNode::ReadSource(v) => {
                    builder = PlanBuilder::from(ctx.clone(), &PlanNode::ReadSource(v.clone()))
                }
                PlanNode::Explain(_v) => {
                    builder = builder.explain()?;
                }
                PlanNode::Select(_v) => {
                    builder = builder.select()?;
                }
                PlanNode::Empty(_) => {}
                PlanNode::Fragment(_) => {}
                PlanNode::Scan(_) => {}
                PlanNode::SetVariable(_) => {}
            }
        }
        builder.build()
    }
}
