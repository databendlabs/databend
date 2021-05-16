// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use crate::AggregatorFinalPlan;
use crate::AggregatorPartialPlan;
use crate::CreateDatabasePlan;
use crate::CreateTablePlan;
use crate::EmptyPlan;
use crate::ExplainPlan;
use crate::ExpressionPlan;
use crate::FilterPlan;
use crate::HavingPlan;
use crate::LimitPlan;
use crate::PlanNode;
use crate::ProjectionPlan;
use crate::ReadDataSourcePlan;
use crate::ScanPlan;
use crate::SelectPlan;
use crate::SettingPlan;
use crate::SortPlan;
use crate::StagePlan;
use crate::UseDatabasePlan;

/// Default plan visitor that can walk through a entire plan.
struct DefaultPlanVisitor {}

impl<'plan> PlanVisitor<'plan> for DefaultPlanVisitor {}

/// `PlanVisitor` implements visitor pattern(reference [syn](https://docs.rs/syn/1.0.72/syn/visit/trait.Visit.html)) for `PlanNode`.
///
/// `PlanVisitor` would provide default implementations for each variant of `PlanNode` to visit a plan tree in preorder.
/// You can customize the way to visit nodes by overriding corresponding methods.
///
/// Since a visitor will always modify itself during visiting, we pass `&mut self` to each visit method.
///
/// # Example
/// Here's an example of printing table names of all `Scan` nodes in a plan tree:
/// ```ignore
/// struct MyVisitor {}
///
/// impl<'plan> PlanVisitor<'plan> for MyVisitor {
///     fn visit_scan(&mut self, plan: &'plan ScanPlan) {
///         println!("{}", plan.schema_name)
///     }
/// }
///
/// let visitor = MyVisitor {};
/// let plan = PlanNode::Scan(ScanPlan {
///     schema_name: "table",
///     ...
/// });
/// visitor.visit_plan_node(&plan); // Output: table
/// ```
pub trait PlanVisitor<'plan> {
    fn visit_plan_node(&mut self, node: &'plan PlanNode) {
        match node {
            PlanNode::AggregatorPartial(plan) => self.visit_aggregate_partial(plan),
            PlanNode::AggregatorFinal(plan) => self.visit_aggregate_final(plan),
            PlanNode::Empty(plan) => self.visit_empty(plan),
            PlanNode::Projection(plan) => self.visit_projection(plan),
            PlanNode::Filter(plan) => self.visit_filter(plan),
            PlanNode::Sort(plan) => self.visit_sort(plan),
            PlanNode::Limit(plan) => self.visit_limit(plan),
            PlanNode::Scan(plan) => self.visit_scan(plan),
            PlanNode::ReadSource(plan) => self.visit_read_data_source(plan),
            PlanNode::Select(plan) => self.visit_select(plan),
            PlanNode::Explain(plan) => self.visit_explain(plan),
            PlanNode::CreateTable(plan) => self.visit_create_table(plan),
            PlanNode::CreateDatabase(plan) => self.visit_create_database(plan),
            PlanNode::UseDatabase(plan) => self.visit_use_database(plan),
            PlanNode::SetVariable(plan) => self.visit_set_variable(plan),
            PlanNode::Stage(plan) => self.visit_stage(plan),
            PlanNode::Having(plan) => self.visit_having(plan),
            PlanNode::Expression(plan) => self.visit_expression(plan)
        }
    }

    fn visit_aggregate_partial(&mut self, plan: &'plan AggregatorPartialPlan) {
        self.visit_plan_node(plan.input.as_ref());
    }

    fn visit_aggregate_final(&mut self, plan: &'plan AggregatorFinalPlan) {
        self.visit_plan_node(plan.input.as_ref());
    }

    fn visit_empty(&mut self, _: &'plan EmptyPlan) {}

    fn visit_stage(&mut self, plan: &'plan StagePlan) {
        self.visit_plan_node(plan.input.as_ref());
    }

    fn visit_projection(&mut self, plan: &'plan ProjectionPlan) {
        self.visit_plan_node(plan.input.as_ref());
    }

    fn visit_expression(&mut self, plan: &'plan ExpressionPlan) {
        self.visit_plan_node(plan.input.as_ref());
    }

    fn visit_filter(&mut self, plan: &'plan FilterPlan) {
        self.visit_plan_node(plan.input.as_ref());
    }

    fn visit_having(&mut self, plan: &'plan HavingPlan) {
        self.visit_plan_node(plan.input.as_ref());
    }

    fn visit_sort(&mut self, plan: &'plan SortPlan) {
        self.visit_plan_node(plan.input.as_ref());
    }

    fn visit_limit(&mut self, plan: &'plan LimitPlan) {
        self.visit_plan_node(plan.input.as_ref());
    }

    fn visit_scan(&mut self, _: &'plan ScanPlan) {}

    fn visit_read_data_source(&mut self, _: &'plan ReadDataSourcePlan) {}

    fn visit_select(&mut self, plan: &'plan SelectPlan) {
        self.visit_plan_node(plan.input.as_ref());
    }

    fn visit_explain(&mut self, plan: &'plan ExplainPlan) {
        self.visit_plan_node(plan.input.as_ref());
    }

    fn visit_create_table(&mut self, _: &'plan CreateTablePlan) {}

    fn visit_create_database(&mut self, _: &'plan CreateDatabasePlan) {}

    fn visit_use_database(&mut self, _: &'plan UseDatabasePlan) {}

    fn visit_set_variable(&mut self, _: &'plan SettingPlan) {}
}
