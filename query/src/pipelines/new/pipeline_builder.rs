use std::sync::Arc;
use petgraph::graph::node_index;
use petgraph::prelude::{NodeIndex, StableGraph};
use poem::http::uri::Port;
use common_planners::{AggregatorFinalPlan, AggregatorPartialPlan, AlterUDFPlan, AlterUserPlan, BroadcastPlan, CopyPlan, CreateDatabasePlan, CreateTablePlan, CreateUDFPlan, CreateUserPlan, CreateUserStagePlan, DescribeStagePlan, DescribeTablePlan, DropDatabasePlan, DropTablePlan, DropUDFPlan, DropUserPlan, DropUserStagePlan, EmptyPlan, ExplainPlan, Expression, ExpressionPlan, FilterPlan, GrantPrivilegePlan, HavingPlan, InsertPlan, KillPlan, LimitByPlan, LimitPlan, OptimizeTablePlan, PlanNode, PlanVisitor, ProjectionPlan, ReadDataSourcePlan, RemotePlan, RevokePrivilegePlan, SelectPlan, SettingPlan, ShowCreateDatabasePlan, ShowCreateTablePlan, ShowGrantsPlan, ShowUDFPlan, SinkPlan, SortPlan, StagePlan, SubQueriesSetPlan, TruncateTablePlan, UseDatabasePlan};
use common_exception::{ErrorCode, Result};
use crate::pipelines::new::pipeline::{Edge, NewPipeline};
use crate::pipelines::new::processors::port::{InputPort, OutputPort};
use crate::pipelines::new::processors::processor::ProcessorPtr;
use crate::pipelines::new::processors::{connect, TableSource};
use crate::sessions::QueryContext;

struct PipelineBuilder {
    ctx: Arc<QueryContext>,
    pipeline: NewPipeline,
}

impl PipelineBuilder {
    pub fn finalize(mut self) -> Result<NewPipeline> {
        Ok(self.pipeline)
    }
}

impl PlanVisitor for PipelineBuilder {
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

        let source_plan = plan.clone();
        let source_context = self.ctx.clone();
        self.pipeline.add_source(
            std::cmp::max(max_threads, 1),
            move |index, output_port| TableSource::try_create(
                Arc::new(output_port),
                source_context.clone(),
                source_plan.clone(),
            ),
        )
    }
}
