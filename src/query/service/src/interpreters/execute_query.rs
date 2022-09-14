use std::sync::Arc;
use common_catalog::table_context::TableContext;
use common_pipeline_core::{Pipe, SourcePipeBuilder};
use crate::interpreters::InterpreterFactory;
use crate::optimizers::Optimizers;
use crate::sessions::QueryContext;
use crate::sql::PlanParser;
use common_exception::{ErrorCode, Result};
use crate::pipelines::executor::{ExecutorSettings, PipelineCompleteExecutor};
use crate::pipelines::PipelineBuildResult;

fn execute_complete(ctx: Arc<QueryContext>, build_res: PipelineBuildResult) -> Result<()> {
    if !build_res.main_pipeline.is_complete_pipeline()? {
        return Err(ErrorCode::LogicalError(""));
    }

    let mut pipelines = build_res.sources_pipelines;
    pipelines.push(build_res.main_pipeline);

    let settings = ctx.get_settings();
    let query_need_abort = ctx.query_need_abort();
    let executor_settings = ExecutorSettings::try_create(&settings)?;
    PipelineCompleteExecutor::from_pipelines(query_need_abort, pipelines, executor_settings)?
        .execute()
}

pub async fn execute_in(ctx: Arc<QueryContext>, query: &str, pipe: Pipe) -> Result<()> {
    match pipe {
        Pipe::ResizePipe { .. } => Err(ErrorCode::LogicalError("")),
        Pipe::SimplePipe { processors, inputs_port, outputs_port } => {
            let plan = PlanParser::parse(ctx.clone(), query).await?;
            let optimized = Optimizers::create(ctx.clone()).optimize(&plan)?;
            let interpreter = InterpreterFactory::get(ctx.clone(), optimized)?;

            let mut source_pipe_builder = SourcePipeBuilder::create();
            for (index, processor) in processors.into_iter().enumerate() {
                source_pipe_builder.add_source(outputs_port[index].clone(), processor);
            }

            interpreter.set_source_pipe_builder(Some(source_pipe_builder))?;
            execute_complete(ctx, interpreter.execute2().await?)
        }
    }
}

pub async fn execute(ctx: Arc<QueryContext>, query: &str) -> Result<()> {
    let plan = PlanParser::parse(ctx.clone(), query).await?;
    let optimized = Optimizers::create(ctx.clone()).optimize(&plan)?;
    let interpreter = InterpreterFactory::get(ctx.clone(), optimized)?;

    match interpreter.execute2().await?.main_pipeline.pipes.is_empty() {
        true => Ok(()),
        false => Err(ErrorCode::LogicalError(""))
    }
}
