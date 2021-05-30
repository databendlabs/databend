// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use crate::AggregatorFinalPlan;
use crate::AggregatorPartialPlan;
use crate::CreateDatabasePlan;
use crate::CreateTablePlan;
use crate::DropDatabasePlan;
use crate::DropTablePlan;
use crate::EmptyPlan;
use crate::ExplainPlan;
use crate::ExpressionPlan;
use crate::FilterPlan;
use crate::HavingPlan;
use crate::JoinPlan;
use crate::InsertIntoPlan;
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
///
/// By default, `PlanVisitor` will visit all `PlanNode` with depth first traversal(i.e. recursively access children of a node).
/// In some cases, people want to explicitly traverse the tree in pre-order or post-order, for whom the default implementation
/// doesn't work. Here we provide an example of pre-order traversal:
/// ```ignore
/// struct PreOrder {
///     pub process: FnMut(&PlanNode)
/// }
///
/// impl<'plan> PlanVisitor<'plan> for PreOrder {
///     fn visit_plan_node(&mut self, plan: &PlanNode) {
///         self.process(plan); // Process current node first
///         PlanVisitor::visit_plan_node(self, plan.child().as_ref()); // Then process children
///     }
/// }
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
            PlanNode::CreateDatabase(plan) => self.visit_create_database(plan),
            PlanNode::DropDatabase(plan) => self.visit_drop_database(plan),
            PlanNode::CreateTable(plan) => self.visit_create_table(plan),
            PlanNode::DropTable(plan) => self.visit_drop_table(plan),
            PlanNode::UseDatabase(plan) => self.visit_use_database(plan),
            PlanNode::SetVariable(plan) => self.visit_set_variable(plan),
            PlanNode::Stage(plan) => self.visit_stage(plan),
            PlanNode::Having(plan) => self.visit_having(plan),
            PlanNode::Expression(plan) => self.visit_expression(plan),
            PlanNode::Join(plan) => self.visit_join(plan),
            PlanNode::InsertInto(plan) => self.visit_insert_into(plan)
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

    fn visit_create_database(&mut self, _: &'plan CreateDatabasePlan) {}

    fn visit_drop_database(&mut self, _: &'plan DropDatabasePlan) {}

    fn visit_create_table(&mut self, _: &'plan CreateTablePlan) {}

    fn visit_drop_table(&mut self, _: &'plan DropTablePlan) {}

    fn visit_use_database(&mut self, _: &'plan UseDatabasePlan) {}

    fn visit_set_variable(&mut self, _: &'plan SettingPlan) {}

    fn visit_join(&mut self, join: &'plan JoinPlan) {
        self.visit_plan_node(join.left_input.as_ref());
        self.visit_plan_node(join.right_input.as_ref());
    }

    fn visit_insert_into(&mut self, _: &'plan InsertIntoPlan) {}
}
