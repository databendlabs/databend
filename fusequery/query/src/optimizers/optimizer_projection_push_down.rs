// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::HashSet;
use std::sync::Arc;

use common_arrow::arrow::error::Result as ArrowResult;
use common_datavalues::DataField;
use common_datavalues::DataSchema;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataSchemaRefExt;
use common_exception::Result;
use common_planners::AggregatorFinalPlan;
use common_planners::AggregatorPartialPlan;
use common_planners::EmptyPlan;
use common_planners::ExpressionAction;
use common_planners::FilterPlan;
use common_planners::PlanNode;
use common_planners::PlanRewriter;
use common_planners::PlanVisitor;
use common_planners::ProjectionPlan;
use common_planners::ReadDataSourcePlan;
use common_planners::SortPlan;

use crate::optimizers::IOptimizer;
use crate::sessions::FuseQueryContextRef;

pub struct ProjectionPushDownOptimizer {}

struct ProjectionPushDownImpl {
    pub required_columns: HashSet<String>,
    pub new_plan: PlanNode,
    pub has_projection: bool,
    pub state: Result<()>
}

impl<'plan> PlanVisitor<'plan> for ProjectionPushDownImpl {
    fn visit_plan_node(&mut self, plan: &PlanNode) {
        match plan {
            PlanNode::AggregatorPartial(plan) => self.visit_aggregate_partial(plan),
            PlanNode::AggregatorFinal(plan) => self.visit_aggregate_final(plan),
            PlanNode::Empty(plan) => self.visit_empty(plan),
            PlanNode::Projection(plan) => self.visit_projection(plan),
            PlanNode::Filter(plan) => self.visit_filter(plan),
            PlanNode::Sort(plan) => self.visit_sort(plan),
            PlanNode::ReadSource(plan) => self.visit_read_data_source(plan),
            _ => self.visit(plan)
        }
    }

    fn visit_projection(&mut self, plan: &ProjectionPlan) {
        if self.state.is_err() {
            return;
        }
        if let Result::Err(e) = self.collect_column_names_from_expr_vec(plan.expr.as_slice()) {
            self.state = Result::Err(e);
            return;
        }
        self.has_projection = true;
        self.visit_plan_node(&plan.input);
        let mut new_plan = plan.clone();
        // TODO: check result
        new_plan.set_input(&self.new_plan).unwrap();
        self.new_plan = PlanNode::Projection(new_plan);
    }

    fn visit_filter(&mut self, plan: &FilterPlan) {
        if self.state.is_err() {
            return;
        }
        if let Result::Err(e) = self.collect_column_names_from_expr(&plan.predicate) {
            self.state = Result::Err(e);
            return;
        }
        self.visit_plan_node(&plan.input);
        let mut new_plan = plan.clone();
        // TODO: check result
        new_plan.set_input(&self.new_plan).unwrap();
        self.new_plan = PlanNode::Filter(new_plan);
    }

    fn visit_aggregate_partial(&mut self, plan: &AggregatorPartialPlan) {
        if self.state.is_err() {
            return;
        }
        if let Result::Err(e) = self
            .collect_column_names_from_expr_vec(&plan.group_expr)
            .and_then(|_| self.collect_column_names_from_expr_vec(&plan.aggr_expr))
        {
            self.state = Result::Err(e);
            return;
        }
        self.visit_plan_node(&plan.input);
        let mut new_plan = plan.clone();
        // TODO: check result
        new_plan.set_input(&self.new_plan).unwrap();
        self.new_plan = PlanNode::AggregatorPartial(new_plan);
    }

    fn visit_aggregate_final(&mut self, plan: &AggregatorFinalPlan) {
        if self.state.is_err() {
            return;
        }
        if let Result::Err(e) = self
            .collect_column_names_from_expr_vec(&plan.group_expr)
            .and_then(|_| self.collect_column_names_from_expr_vec(&plan.aggr_expr))
        {
            self.state = Result::Err(e);
            return;
        }
        self.visit_plan_node(&plan.input);
        let mut new_plan = plan.clone();
        // TODO: check result
        new_plan.set_input(&self.new_plan).unwrap();
        self.new_plan = PlanNode::AggregatorFinal(new_plan);
    }

    fn visit_sort(&mut self, plan: &SortPlan) {
        if self.state.is_err() {
            return;
        }
        if let Result::Err(e) = self.collect_column_names_from_expr_vec(&plan.order_by) {
            self.state = Result::Err(e);
            return;
        }
        self.visit_plan_node(&plan.input);
        let mut new_plan = plan.clone();
        // TODO: check result
        new_plan.set_input(&self.new_plan).unwrap();
        self.new_plan = PlanNode::Sort(new_plan);
    }

    fn visit_read_data_source(&mut self, plan: &ReadDataSourcePlan) {
        if self.state.is_err() {
            return;
        }
        match self.get_projected_schema(plan.schema.as_ref()) {
            Ok(projected_schema) => {
                self.new_plan = PlanNode::ReadSource(ReadDataSourcePlan {
                    db: plan.db.to_string(),
                    table: plan.table.to_string(),
                    schema: projected_schema,
                    partitions: plan.partitions.clone(),
                    statistics: plan.statistics.clone(),
                    description: plan.description.to_string()
                })
            }
            Err(e) => {
                self.state = Result::Err(e);
            }
        }
    }

    fn visit_empty(&mut self, plan: &EmptyPlan) {
        if self.state.is_err() {
            return;
        }
        self.new_plan = PlanNode::Empty(plan.clone());
    }
}

impl ProjectionPushDownImpl {
    pub fn new() -> ProjectionPushDownImpl {
        ProjectionPushDownImpl {
            required_columns: HashSet::new(),
            new_plan: PlanNode::Empty(EmptyPlan {
                schema: Arc::new(DataSchema::new(vec![]))
            }),
            has_projection: false,
            state: Ok(())
        }
    }

    /// Finalize plan after ProjectionPushDown optimization.
    /// This function should only be called after calling `visit`
    fn finalize(self) -> Result<PlanNode> {
        match self.state {
            Ok(()) => Ok(self.new_plan),
            Err(e) => Err(e)
        }
    }

    /// Visit the plan tree and do ProjectionPushDown optimization.
    fn visit(&mut self, plan: &PlanNode) {
        if self.state.is_err() {
            return;
        }
        self.visit_plan_node(&plan.input());
        let mut new_plan = plan.clone();
        // TODO: check result
        new_plan.set_input(&self.new_plan).unwrap();
        self.new_plan = new_plan;
    }

    // Recursively walk a list of expression trees, collecting the unique set of column
    // names referenced in the expression
    fn collect_column_names_from_expr_vec(&mut self, expr: &[ExpressionAction]) -> Result<()> {
        expr.iter().fold(Ok(()), |acc, e| {
            acc.and_then(|_| self.collect_column_names_from_expr(e))
        })
    }

    // Recursively walk an expression tree, collecting the unique set of column names
    // referenced in the expression
    fn collect_column_names_from_expr(&mut self, expr: &ExpressionAction) -> Result<()> {
        PlanRewriter::expression_plan_children(expr)?
            .iter()
            .fold(Ok(()), |acc, e| {
                acc.and_then(|_| self.collect_column_names_from_expr(e))
            })?;

        if let ExpressionAction::Column(name) = expr {
            self.required_columns.insert(name.clone());
        }
        Ok(())
    }

    fn get_projected_schema(&self, schema: &DataSchema) -> Result<DataSchemaRef> {
        // Discard non-existing columns, e.g. when the column derives from aggregation
        let mut projection: Vec<usize> = self
            .required_columns
            .iter()
            .map(|name| schema.index_of(name))
            .filter_map(ArrowResult::ok)
            .collect();
        if projection.is_empty() {
            if self.has_projection {
                // Ensure reading at lease one column
                projection.push(0);
            } else {
                // for table scan without projection
                // just return all columns
                projection = schema
                    .fields()
                    .iter()
                    .enumerate()
                    .map(|(i, _)| i)
                    .collect::<Vec<usize>>();
            }
        }
        // sort the projection to get deterministic behavior
        projection.sort_unstable();

        // create the projected schema
        let mut projected_fields: Vec<DataField> = Vec::with_capacity(projection.len());
        for i in &projection {
            projected_fields.push(schema.fields()[*i].clone());
        }
        Ok(DataSchemaRefExt::create(projected_fields))
    }
}

impl IOptimizer for ProjectionPushDownOptimizer {
    fn name(&self) -> &str {
        "ProjectionPushDown"
    }

    fn optimize(&mut self, plan: &PlanNode) -> Result<PlanNode> {
        let mut visitor = ProjectionPushDownImpl::new();
        visitor.visit(plan);
        visitor.finalize()
    }
}

impl ProjectionPushDownOptimizer {
    pub fn create(_ctx: FuseQueryContextRef) -> ProjectionPushDownOptimizer {
        ProjectionPushDownOptimizer {}
    }
}
