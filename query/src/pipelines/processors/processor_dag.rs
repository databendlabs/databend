use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::AggregatorFinalPlan;
use common_planners::AggregatorPartialPlan;
use common_planners::ExpressionPlan;
use common_planners::FilterPlan;
use common_planners::HavingPlan;
use common_planners::LimitByPlan;
use common_planners::LimitPlan;
use common_planners::PlanNode;
use common_planners::ProjectionPlan;
use common_planners::ReadDataSourcePlan;
use common_planners::RemotePlan;
use common_planners::SelectPlan;
use common_planners::SortPlan;
use petgraph::dot::Config;
use petgraph::dot::Dot;
use petgraph::prelude::NodeIndex;
use petgraph::prelude::StableGraph;

use crate::api::FlightTicket;
use crate::pipelines::processors::processor::ProcessorRef;
use crate::pipelines::processors::Processor;
use crate::pipelines::transforms::AggregatorFinalTransform;
use crate::pipelines::transforms::AggregatorPartialTransform;
use crate::pipelines::transforms::ExpressionTransform;
use crate::pipelines::transforms::GroupByFinalTransform;
use crate::pipelines::transforms::GroupByPartialTransform;
use crate::pipelines::transforms::HavingTransform;
use crate::pipelines::transforms::LimitByTransform;
use crate::pipelines::transforms::LimitTransform;
use crate::pipelines::transforms::ProjectionTransform;
use crate::pipelines::transforms::RemoteTransform;
use crate::pipelines::transforms::SortMergeTransform;
use crate::pipelines::transforms::SortPartialTransform;
use crate::pipelines::transforms::SourceTransform;
use crate::pipelines::transforms::WhereTransform;
use crate::sessions::DatabendQueryContextRef;

pub struct ProcessorsDAG {
    pub(in crate::pipelines::processors) graph: StableGraph<Arc<dyn Processor>, ()>,
}

impl ProcessorsDAG {
    pub fn create(graph: StableGraph<Arc<dyn Processor>, ()>) -> ProcessorsDAG {
        ProcessorsDAG { graph }
    }
}

impl Debug for ProcessorsDAG {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:?}",
            Dot::with_config(&self.graph, &[Config::EdgeNoLabel])
        )
    }
}

// TODO(Winter): import distributed processors in DAG. We can use edge to describe them. e.g. enum EdgeAttrs { Local, Remote }
pub struct ProcessorsDAGBuilder {
    ctx: DatabendQueryContextRef,
    graph: StableGraph<Arc<dyn Processor>, ()>,
    top_processors: Vec<NodeIndex>,

    // TODO(Winter): remove this.
    limit: Option<usize>,
    after_order_by: bool,
}

impl ProcessorsDAGBuilder {
    pub fn create(ctx: DatabendQueryContextRef) -> ProcessorsDAGBuilder {
        ProcessorsDAGBuilder {
            ctx,
            graph: Default::default(),
            top_processors: vec![],
            limit: None,
            after_order_by: false,
        }
    }

    pub fn build(mut self, node: &PlanNode) -> Result<ProcessorsDAG> {
        self.visit(node)?;
        Ok(ProcessorsDAG::create(self.graph))
    }

    fn visit(&mut self, node: &PlanNode) -> Result<()> {
        match node {
            PlanNode::Select(node) => self.visit_select(node),
            PlanNode::Filter(node) => self.visit_filter(node),
            PlanNode::Having(node) => self.visit_having(node),
            PlanNode::Projection(node) => self.visit_projection(node),
            PlanNode::Remote(node) => self.visit_remote_source(node),
            PlanNode::Expression(node) => self.visit_expression(node),
            PlanNode::AggregatorPartial(node) => self.visit_aggregator_partial(node),
            PlanNode::AggregatorFinal(node) => self.visit_aggregator_final(node),
            PlanNode::Sort(node) => self.visit_sort(node),
            PlanNode::Limit(node) => self.visit_limit(node),
            PlanNode::LimitBy(node) => self.visit_limit_by(node),
            PlanNode::ReadSource(node) => self.visit_read_data_source(node),
            // PlanNode::SubQueryExpression(node) => self.visit_create_sets(node),
            other => Result::Err(ErrorCode::UnknownPlan(format!(
                "Build processors DAG from the plan node unsupported:{:?}",
                other.name()
            ))),
        }
    }

    fn visit_select(&mut self, node: &SelectPlan) -> Result<()> {
        self.visit(&*node.input)
    }

    fn visit_sort(&mut self, plan: &SortPlan) -> Result<()> {
        self.visit(&*plan.input)?;

        self.visit_sort_for_partial(plan)?;
        self.visit_sort_for_merge_with_single_thread(plan)?;
        self.visit_sort_for_merge_with_multiple_threads(plan)?;
        self.after_order_by = true;
        Ok(())
    }

    fn visit_limit(&mut self, node: &LimitPlan) -> Result<()> {
        self.visit(&*node.input)?;

        self.limit = node.n;
        self.add_merge_graph_node(|| {
            let limit = node.n;
            let offset = node.offset;
            Ok(Arc::new(LimitTransform::try_create(limit, offset)?))
        })
    }

    fn visit_limit_by(&mut self, node: &LimitByPlan) -> Result<()> {
        self.visit(&*node.input)?;

        self.add_merge_graph_node(|| {
            let limit = node.limit;
            let limit_by_desc = node.limit_by.clone();
            Ok(Arc::new(LimitByTransform::create(limit, limit_by_desc)))
        })
    }

    fn visit_sort_for_partial(&mut self, plan: &SortPlan) -> Result<()> {
        // processor 1: block ---> sort_stream
        // processor 2: block ---> sort_stream
        // processor 3: block ---> sort_stream
        let limit = self.limit;
        self.add_auto_graph_node(move || {
            let schema = plan.schema();
            let order_by_desc = plan.order_by.clone();
            Ok(Arc::new(SortPartialTransform::try_create(
                schema,
                order_by_desc,
                limit,
            )?))
        })
    }

    fn visit_sort_for_merge_with_single_thread(&mut self, plan: &SortPlan) -> Result<()> {
        // processor 1: [sorted blocks ...] ---> merge to one sorted block
        // processor 2: [sorted blocks ...] ---> merge to one sorted block
        // processor 3: [sorted blocks ...] ---> merge to one sorted block
        let limit = self.limit;
        self.add_auto_graph_node(move || {
            let schema = plan.schema();
            let order_by_desc = plan.order_by.clone();
            Ok(Arc::new(SortMergeTransform::try_create(
                schema,
                order_by_desc,
                limit,
            )?))
        })
    }

    fn visit_sort_for_merge_with_multiple_threads(&mut self, plan: &SortPlan) -> Result<()> {
        // processor1 sorted block --
        //                             \
        // processor2 sorted block ----> processor  --> merge to one sorted block
        //                             /
        // processor3 sorted block --
        let limit = self.limit;
        self.add_merge_graph_node(|| {
            let schema = plan.schema();
            let order_by_desc = plan.order_by.clone();
            Ok(Arc::new(SortMergeTransform::try_create(
                schema,
                order_by_desc,
                limit,
            )?))
        })
    }

    fn visit_filter(&mut self, node: &FilterPlan) -> Result<()> {
        self.visit(&*node.input)?;
        self.add_auto_graph_node(|| {
            let schema = node.schema();
            let predicate = node.predicate.clone();
            Ok(Arc::new(WhereTransform::try_create(schema, predicate)?))
        })
    }

    fn visit_having(&mut self, node: &HavingPlan) -> Result<()> {
        self.visit(&*node.input)?;
        self.add_auto_graph_node(|| {
            let schema = node.schema();
            let predicate = node.predicate.clone();
            Ok(Arc::new(HavingTransform::try_create(schema, predicate)?))
        })
    }

    fn visit_expression(&mut self, plan: &ExpressionPlan) -> Result<()> {
        self.visit(&*plan.input)?;
        self.add_auto_graph_node(|| {
            let exprs = plan.exprs.clone();
            let input = plan.input.schema();
            let output = plan.schema.clone();
            Ok(Arc::new(ExpressionTransform::try_create(
                input, output, exprs,
            )?))
        })
    }

    fn visit_projection(&mut self, node: &ProjectionPlan) -> Result<()> {
        self.visit(&*node.input)?;
        self.add_auto_graph_node(|| {
            let exprs = node.expr.clone();
            let input = node.input.schema();
            let output = node.schema.clone();
            Ok(Arc::new(ProjectionTransform::try_create(
                input, output, exprs,
            )?))
        })
    }

    fn visit_aggregator_final(&mut self, node: &AggregatorFinalPlan) -> Result<()> {
        self.visit(&*node.input)?;

        if node.group_expr.is_empty() {
            self.add_merge_graph_node(|| {
                Ok(Arc::new(AggregatorFinalTransform::try_create(
                    node.schema(),
                    node.schema_before_group_by.clone(),
                    node.aggr_expr.clone(),
                )?))
            })
        } else {
            let max_block_size = self.ctx.get_settings().get_max_block_size()? as usize;
            self.add_merge_graph_node(|| {
                Ok(Arc::new(GroupByFinalTransform::create(
                    node.schema(),
                    max_block_size,
                    node.schema_before_group_by.clone(),
                    node.aggr_expr.clone(),
                    node.group_expr.clone(),
                )))
            })
        }
    }

    fn visit_aggregator_partial(&mut self, node: &AggregatorPartialPlan) -> Result<()> {
        self.visit(&*node.input)?;

        if node.group_expr.is_empty() {
            self.add_auto_graph_node(|| {
                Ok(Arc::new(AggregatorPartialTransform::try_create(
                    node.schema(),
                    node.input.schema(),
                    node.aggr_expr.clone(),
                )?))
            })
        } else {
            self.add_auto_graph_node(|| {
                Ok(Arc::new(GroupByPartialTransform::create(
                    node.schema(),
                    node.input.schema(),
                    node.aggr_expr.clone(),
                    node.group_expr.clone(),
                )))
            })
        }
    }

    fn visit_remote_source(&mut self, plan: &RemotePlan) -> Result<()> {
        if !self.top_processors.is_empty() {
            return Err(ErrorCode::LogicalError(
                "Logical error: index not empty(while add remote source).",
            ));
        }

        for fetch_node in &plan.fetch_nodes {
            let query_id = &plan.query_id;
            let stage_id = &plan.stage_id;
            let stream_id = &plan.stream_id;
            let flight_ticket = FlightTicket::stream(query_id, stage_id, stream_id);

            let remote_context = self.ctx.clone();
            let remote_transform = RemoteTransform::try_create(
                flight_ticket,
                remote_context,
                /* fetch_node_name */ fetch_node.clone(),
                /* fetch_stream_schema */ plan.schema.clone(),
            )?;

            self.top_processors
                .push(self.graph.add_node(Arc::new(remote_transform)));
        }

        Ok(())
    }

    fn visit_read_data_source(&mut self, plan: &ReadDataSourcePlan) -> Result<()> {
        // Bind plan partitions to context.
        self.ctx.try_set_partitions(plan.parts.clone())?;

        let max_threads = self.ctx.get_settings().get_max_threads()? as usize;
        let max_threads = std::cmp::min(max_threads, plan.parts.len());

        if !self.top_processors.is_empty() {
            return Err(ErrorCode::LogicalError(
                "Logical error: index not empty(while add source).",
            ));
        }

        for _source_index in 0..std::cmp::max(max_threads, 1) {
            let source_plan = plan.clone();
            let source_ctx = self.ctx.clone();
            let source_transform = SourceTransform::try_create(source_ctx, source_plan)?;
            self.top_processors
                .push(self.graph.add_node(Arc::new(source_transform)));
        }

        Ok(())
    }
}

// Some DAG API implement
impl ProcessorsDAGBuilder {
    fn add_merge_graph_node<F: Fn() -> Result<ProcessorRef>>(&mut self, f: F) -> Result<()> {
        let node = f()?;
        let to_index = self.graph.add_node(node);

        for index in 0..self.top_processors.len() {
            self.graph
                .add_edge(self.top_processors[index], to_index, ());
        }

        self.top_processors.clear();
        self.top_processors.push(to_index);
        Ok(())
    }

    fn add_simple_graph_node<F: Fn() -> Result<ProcessorRef>>(&mut self, f: F) -> Result<()> {
        for index in 0..self.top_processors.len() {
            let node = f()?;
            let to_index = self.graph.add_node(node);
            self.graph
                .add_edge(self.top_processors[index], to_index, ());
            self.top_processors[index] = to_index;
        }

        Ok(())
    }

    fn add_auto_graph_node<F: Fn() -> Result<ProcessorRef>>(&mut self, f: F) -> Result<()> {
        if self.after_order_by {
            return self.add_simple_graph_node(f);
        }

        let max_threads = self.ctx.get_settings().get_max_threads()? as usize;

        match self.top_processors.len() == max_threads {
            true => self.add_simple_graph_node(f),
            false => self.add_mixed_graph_node(f),
        }
    }

    fn add_mixed_graph_node<F: Fn() -> Result<ProcessorRef>>(&mut self, f: F) -> Result<()> {
        let max_threads = self.ctx.get_settings().get_max_threads()? as usize;
        let mut new_top_processors = Vec::with_capacity(max_threads);

        for _thread_num in 0..max_threads {
            let node = f()?;
            let to_index = self.graph.add_node(node);

            for index in 0..self.top_processors.len() {
                self.graph
                    .add_edge(self.top_processors[index], to_index, ());
            }

            new_top_processors.push(to_index);
        }

        self.top_processors = new_top_processors;
        Ok(())
    }
}
