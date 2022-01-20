// Copyright 2022 Datafuse Labs.
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

use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::FilterPlan;
use common_planners::PlanNode;
use common_planners::PlanVisitor;
use common_planners::ReadDataSourcePlan;

use crate::pipelines::new::pipe::SourcePipeBuilder;
use crate::pipelines::new::pipeline::NewPipeline;
use crate::pipelines::new::processors::port::OutputPort;
use crate::pipelines::new::processors::TableSource;
use crate::sessions::QueryContext;

#[allow(dead_code)]
struct QueryPipelineBuilder {
    ctx: Arc<QueryContext>,
    pipeline: NewPipeline,
}

#[allow(dead_code)]
impl QueryPipelineBuilder {
    pub fn create(ctx: Arc<QueryContext>) -> QueryPipelineBuilder {
        QueryPipelineBuilder {
            ctx,
            pipeline: NewPipeline::create(),
        }
    }

    pub fn finalize(self) -> Result<NewPipeline> {
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

        let mut source_builder = SourcePipeBuilder::create();
        for _index in 0..std::cmp::max(max_threads, 1) {
            let source_plan = plan.clone();
            let source_ctx = self.ctx.clone();

            let source_output_port = OutputPort::create();

            source_builder.add_source(
                TableSource::try_create(source_output_port.clone(), source_ctx, source_plan)?,
                source_output_port,
            );
        }

        self.pipeline.add_pipe(source_builder.finalize());
        Ok(())
    }
}
