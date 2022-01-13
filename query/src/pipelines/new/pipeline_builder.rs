use std::sync::Arc;
use petgraph::graph::node_index;
use petgraph::prelude::{NodeIndex, StableGraph};
use poem::http::uri::Port;
use common_exception::{ErrorCode, Result};
use common_planners::{FilterPlan, PlanNode, PlanVisitor, ReadDataSourcePlan};
use crate::pipelines::new::pipeline::{Edge, NewPipe, NewPipeline};
use crate::pipelines::new::processors::port::{InputPort, OutputPort};
use crate::pipelines::new::processors::processor::ProcessorPtr;
use crate::pipelines::new::processors::{TableSource, UpdateList};
use crate::pipelines::processors::Processor;
use crate::sessions::QueryContext;

struct QueryPipelineBuilder {
    ctx: Arc<QueryContext>,
    pipeline: NewPipeline,
}

impl QueryPipelineBuilder {
    pub fn finalize(mut self) -> Result<NewPipeline> {
        Ok(self.pipeline)
    }
}

impl PlanVisitor for QueryPipelineBuilder {
    fn visit_plan_node(&mut self, node: &PlanNode) -> Result<()> {
        match node {
            PlanNode::Projection(n) => self.visit_projection(n),
            PlanNode::Expression(n) => self.visit_expression(n),
            PlanNode::AggregatorPartial(n) => self.visit_aggregate_partial(n),
            PlanNode::AggregatorFinal(n) => self.visit_aggregate_final(n),
            PlanNode::Filter(n) => self.visit_filter(n),
            PlanNode::Having(n) => self.visit_having(n),
            PlanNode::Sort(n) => self.visit_sort(n),
            PlanNode::Limit(n) => self.visit_limit(n),
            PlanNode::LimitBy(n) => self.visit_limit_by(n),
            PlanNode::ReadSource(n) => self.visit_read_data_source(n),
            PlanNode::Select(n) => self.visit_select(n),
            PlanNode::Explain(n) => self.visit_explain(n),
            _ => Err(ErrorCode::UnImplement("")),
        }
    }

    fn visit_filter(&mut self, plan: &FilterPlan) -> Result<()> {
        self.visit_plan_node(&plan.input)?;
        // TODO:
        Ok(())
    }

    fn visit_read_data_source(&mut self, plan: &ReadDataSourcePlan) -> Result<()> {
        // Bind plan partitions to context.
        self.ctx.try_set_partitions(plan.parts.clone())?;

        let max_threads = self.ctx.get_settings().get_max_threads()? as usize;
        let max_threads = std::cmp::min(max_threads, plan.parts.len());

        for _index in 0..std::cmp::max(max_threads, 1) {
            let source_plan = plan.clone();
            let source_ctx = self.ctx.clone();

            let update_list = UpdateList::create();
            let source_output_port = OutputPort::create(update_list.clone());

            let scan_table_source = TableSource::try_create(Arc::new(source_output_port.clone()), source_ctx, source_plan)?;
            Ok(NewPipe::create_source(scan_table_source, source_output_port))
        }

        self.pipes_creator.push(pipe_creators);

        Ok(())
    }
}
