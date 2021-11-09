// Copyright 2020 Datafuse Labs.
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

use common_exception::Result;

use crate::plan_broadcast::BroadcastPlan;
use crate::plan_subqueries_set::SubQueriesSetPlan;
use crate::AggregatorFinalPlan;
use crate::AggregatorPartialPlan;
use crate::CreateDatabasePlan;
use crate::CreateTablePlan;
use crate::CreateUserPlan;
use crate::DescribeTablePlan;
use crate::DropDatabasePlan;
use crate::DropTablePlan;
use crate::EmptyPlan;
use crate::ExplainPlan;
use crate::Expression;
use crate::ExpressionPlan;
use crate::FilterPlan;
use crate::HavingPlan;
use crate::InsertIntoPlan;
use crate::KillPlan;
use crate::LimitByPlan;
use crate::LimitPlan;
use crate::PlanNode;
use crate::ProjectionPlan;
use crate::ReadDataSourcePlan;
use crate::RemotePlan;
use crate::SelectPlan;
use crate::SettingPlan;
use crate::ShowCreateTablePlan;
use crate::SortPlan;
use crate::StagePlan;
use crate::TruncateTablePlan;
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
///     fn visit_read_data_source(&mut self, plan: &'plan ReadDataSourcePlan) {
///         println!("{}", plan.schema_name)
///     }
/// }
///
/// let visitor = MyVisitor {};
/// let plan = PlanNode::ReadDataSource(ReadDataSourcePlan {
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
pub trait PlanVisitor {
    fn visit_plan_node(&mut self, node: &PlanNode) -> Result<()> {
        match node {
            PlanNode::AggregatorPartial(plan) => self.visit_aggregate_partial(plan),
            PlanNode::AggregatorFinal(plan) => self.visit_aggregate_final(plan),
            PlanNode::Empty(plan) => self.visit_empty(plan),
            PlanNode::Projection(plan) => self.visit_projection(plan),
            PlanNode::Filter(plan) => self.visit_filter(plan),
            PlanNode::Sort(plan) => self.visit_sort(plan),
            PlanNode::Limit(plan) => self.visit_limit(plan),
            PlanNode::LimitBy(plan) => self.visit_limit_by(plan),
            PlanNode::ReadSource(plan) => self.visit_read_data_source(plan),
            PlanNode::Select(plan) => self.visit_select(plan),
            PlanNode::Explain(plan) => self.visit_explain(plan),
            PlanNode::CreateDatabase(plan) => self.visit_create_database(plan),
            PlanNode::DropDatabase(plan) => self.visit_drop_database(plan),
            PlanNode::CreateTable(plan) => self.visit_create_table(plan),
            PlanNode::DropTable(plan) => self.visit_drop_table(plan),
            PlanNode::DescribeTable(plan) => self.visit_describe_table(plan),
            PlanNode::TruncateTable(plan) => self.visit_truncate_table(plan),
            PlanNode::UseDatabase(plan) => self.visit_use_database(plan),
            PlanNode::SetVariable(plan) => self.visit_set_variable(plan),
            PlanNode::Stage(plan) => self.visit_stage(plan),
            PlanNode::Broadcast(plan) => self.visit_broadcast(plan),
            PlanNode::Remote(plan) => self.visit_remote(plan),
            PlanNode::Having(plan) => self.visit_having(plan),
            PlanNode::Expression(plan) => self.visit_expression(plan),
            PlanNode::InsertInto(plan) => self.visit_insert_into(plan),
            PlanNode::ShowCreateTable(plan) => self.visit_show_create_table(plan),
            PlanNode::SubQueryExpression(plan) => self.visit_sub_queries_sets(plan),
            PlanNode::Kill(plan) => self.visit_kill_query(plan),
            PlanNode::CreateUser(plan) => self.visit_create_user(plan),
        }
    }

    fn visit_subquery_plan(&mut self, subquery_plan: &PlanNode) -> Result<()> {
        self.visit_plan_node(subquery_plan)
    }

    // TODO: Move it to ExpressionsVisitor trait
    fn visit_expr(&mut self, expr: &Expression) -> Result<()> {
        match expr {
            Expression::Subquery { query_plan, .. } => {
                self.visit_subquery_plan(query_plan.as_ref())
            }
            Expression::ScalarSubquery { query_plan, .. } => {
                self.visit_subquery_plan(query_plan.as_ref())
            }
            _ => Ok(()),
        }
    }

    // TODO: Move it to ExpressionsVisitor trait
    fn visit_exprs(&mut self, exprs: &[Expression]) -> Result<()> {
        for expr in exprs {
            self.visit_expr(expr)?;
        }

        Ok(())
    }

    fn visit_aggregate_partial(&mut self, plan: &AggregatorPartialPlan) -> Result<()> {
        self.visit_plan_node(plan.input.as_ref())?;
        self.visit_exprs(&plan.aggr_expr)?;
        self.visit_exprs(&plan.group_expr)
    }

    fn visit_aggregate_final(&mut self, plan: &AggregatorFinalPlan) -> Result<()> {
        self.visit_plan_node(plan.input.as_ref())?;
        self.visit_exprs(&plan.aggr_expr)?;
        self.visit_exprs(&plan.group_expr)
    }

    fn visit_empty(&mut self, _: &EmptyPlan) -> Result<()> {
        Ok(())
    }

    fn visit_stage(&mut self, plan: &StagePlan) -> Result<()> {
        self.visit_plan_node(plan.input.as_ref())
    }

    fn visit_broadcast(&mut self, plan: &BroadcastPlan) -> Result<()> {
        self.visit_plan_node(plan.input.as_ref())
    }

    fn visit_remote(&mut self, _: &RemotePlan) -> Result<()> {
        Ok(())
    }

    fn visit_projection(&mut self, plan: &ProjectionPlan) -> Result<()> {
        self.visit_plan_node(plan.input.as_ref())?;
        self.visit_exprs(&plan.expr)
    }

    fn visit_expression(&mut self, plan: &ExpressionPlan) -> Result<()> {
        self.visit_plan_node(plan.input.as_ref())?;
        self.visit_exprs(&plan.exprs)
    }

    fn visit_sub_queries_sets(&mut self, plan: &SubQueriesSetPlan) -> Result<()> {
        self.visit_plan_node(plan.input.as_ref())?;
        self.visit_exprs(&plan.expressions)
    }

    fn visit_filter(&mut self, plan: &FilterPlan) -> Result<()> {
        self.visit_plan_node(plan.input.as_ref())?;
        self.visit_expr(&plan.predicate)
    }

    fn visit_having(&mut self, plan: &HavingPlan) -> Result<()> {
        self.visit_plan_node(plan.input.as_ref())?;
        self.visit_expr(&plan.predicate)
    }

    fn visit_sort(&mut self, plan: &SortPlan) -> Result<()> {
        self.visit_plan_node(plan.input.as_ref())?;
        self.visit_exprs(&plan.order_by)
    }

    fn visit_limit(&mut self, plan: &LimitPlan) -> Result<()> {
        self.visit_plan_node(plan.input.as_ref())
    }

    fn visit_limit_by(&mut self, plan: &LimitByPlan) -> Result<()> {
        self.visit_plan_node(plan.input.as_ref())
    }

    fn visit_read_data_source(&mut self, _: &ReadDataSourcePlan) -> Result<()> {
        Ok(())
    }

    fn visit_select(&mut self, plan: &SelectPlan) -> Result<()> {
        self.visit_plan_node(plan.input.as_ref())
    }

    fn visit_explain(&mut self, plan: &ExplainPlan) -> Result<()> {
        self.visit_plan_node(plan.input.as_ref())
    }

    fn visit_create_database(&mut self, _: &CreateDatabasePlan) -> Result<()> {
        Ok(())
    }

    fn visit_drop_database(&mut self, _: &DropDatabasePlan) -> Result<()> {
        Ok(())
    }

    fn visit_create_table(&mut self, _: &CreateTablePlan) -> Result<()> {
        Ok(())
    }

    fn visit_create_user(&mut self, _: &CreateUserPlan) -> Result<()> {
        Ok(())
    }

    fn visit_describe_table(&mut self, _: &DescribeTablePlan) -> Result<()> {
        Ok(())
    }

    fn visit_drop_table(&mut self, _: &DropTablePlan) -> Result<()> {
        Ok(())
    }

    fn visit_use_database(&mut self, _: &UseDatabasePlan) -> Result<()> {
        Ok(())
    }

    fn visit_set_variable(&mut self, _: &SettingPlan) -> Result<()> {
        Ok(())
    }

    fn visit_insert_into(&mut self, _: &InsertIntoPlan) -> Result<()> {
        Ok(())
    }

    fn visit_show_create_table(&mut self, _: &ShowCreateTablePlan) -> Result<()> {
        Ok(())
    }

    fn visit_truncate_table(&mut self, _: &TruncateTablePlan) -> Result<()> {
        Ok(())
    }

    fn visit_kill_query(&mut self, _: &KillPlan) -> Result<()> {
        Ok(())
    }
}
