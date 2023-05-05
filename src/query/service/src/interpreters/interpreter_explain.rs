// Copyright 2021 Datafuse Labs
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

use common_ast::ast::ExplainKind;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::DataType;
use common_expression::types::StringType;
use common_expression::DataBlock;
use common_expression::DataField;
use common_expression::DataSchemaRef;
use common_expression::DataSchemaRefExt;
use common_expression::FromData;
use common_profile::ProfSpanSetRef;
use common_sql::MetadataRef;

use crate::interpreters::Interpreter;
use crate::pipelines::executor::ExecutorSettings;
use crate::pipelines::executor::PipelineCompleteExecutor;
use crate::pipelines::executor::PipelinePullingExecutor;
use crate::pipelines::PipelineBuildResult;
use crate::schedulers::build_query_pipeline;
use crate::schedulers::Fragmenter;
use crate::schedulers::QueryFragmentsActions;
use crate::sessions::QueryContext;
use crate::sql::executor::PhysicalPlan;
use crate::sql::executor::PhysicalPlanBuilder;
use crate::sql::optimizer::SExpr;
use crate::sql::plans::Plan;

pub struct ExplainInterpreter {
    ctx: Arc<QueryContext>,
    schema: DataSchemaRef,
    kind: ExplainKind,
    plan: Plan,
}

#[async_trait::async_trait]
impl Interpreter for ExplainInterpreter {
    fn name(&self) -> &str {
        "ExplainInterpreterV2"
    }

    fn schema(&self) -> DataSchemaRef {
        self.schema.clone()
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let blocks = match &self.kind {
            ExplainKind::Raw => self.explain_plan(&self.plan)?,

            ExplainKind::Plan => match &self.plan {
                Plan::Query {
                    s_expr, metadata, ..
                } => {
                    let ctx = self.ctx.clone();
                    let settings = ctx.get_settings();

                    let enable_distributed_eval_index =
                        settings.get_enable_distributed_eval_index()?;
                    settings.set_enable_distributed_eval_index(false)?;
                    scopeguard::defer! {
                        let _ = settings.set_enable_distributed_eval_index(enable_distributed_eval_index);
                    }
                    let mut builder = PhysicalPlanBuilder::new(metadata.clone(), ctx);
                    let plan = builder.build(s_expr).await?;
                    self.explain_physical_plan(&plan, metadata)?
                }
                _ => self.explain_plan(&self.plan)?,
            },

            ExplainKind::JOIN => match &self.plan {
                Plan::Query {
                    s_expr, metadata, ..
                } => {
                    let ctx = self.ctx.clone();
                    let settings = ctx.get_settings();

                    let enable_distributed_eval_index =
                        settings.get_enable_distributed_eval_index()?;
                    settings.set_enable_distributed_eval_index(false)?;
                    scopeguard::defer! {
                        let _ = settings.set_enable_distributed_eval_index(enable_distributed_eval_index);
                    }
                    let mut builder = PhysicalPlanBuilder::new(metadata.clone(), ctx);
                    let plan = builder.build(s_expr).await?;
                    self.explain_join_order(&plan, metadata)?
                }
                _ => Err(ErrorCode::Unimplemented(
                    "Unsupported EXPLAIN JOIN statement",
                ))?,
            },

            ExplainKind::AnalyzePlan => match &self.plan {
                Plan::Query {
                    s_expr,
                    metadata,
                    ignore_result,
                    ..
                } => {
                    let ctx = self.ctx.clone();
                    let settings = ctx.get_settings();

                    let enable_distributed_eval_index =
                        settings.get_enable_distributed_eval_index()?;
                    settings.set_enable_distributed_eval_index(false)?;
                    scopeguard::defer! {
                        let _ = settings.set_enable_distributed_eval_index(enable_distributed_eval_index);
                    }

                    self.explain_analyze(s_expr, metadata, *ignore_result)
                        .await?
                }
                _ => Err(ErrorCode::Unimplemented(
                    "Unsupported EXPLAIN ANALYZE statement",
                ))?,
            },

            ExplainKind::Pipeline => match &self.plan {
                Plan::Query {
                    s_expr,
                    metadata,
                    ignore_result,
                    ..
                } => {
                    self.explain_pipeline(*s_expr.clone(), metadata.clone(), *ignore_result)
                        .await?
                }
                _ => {
                    return Err(ErrorCode::Unimplemented("Unsupported EXPLAIN statement"));
                }
            },

            ExplainKind::Fragments => match &self.plan {
                Plan::Query {
                    s_expr, metadata, ..
                } => {
                    self.explain_fragments(*s_expr.clone(), metadata.clone())
                        .await?
                }
                _ => {
                    return Err(ErrorCode::Unimplemented("Unsupported EXPLAIN statement"));
                }
            },

            ExplainKind::Graph => {
                return Err(ErrorCode::Unimplemented(
                    "ExplainKind graph is unimplemented",
                ));
            }

            ExplainKind::Ast(display_string)
            | ExplainKind::Syntax(display_string)
            | ExplainKind::Memo(display_string) => {
                let line_split_result: Vec<&str> = display_string.lines().collect();
                let column = StringType::from_data(line_split_result);
                vec![DataBlock::new_from_columns(vec![column])]
            }
        };

        PipelineBuildResult::from_blocks(blocks)
    }
}

impl ExplainInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: Plan, kind: ExplainKind) -> Result<Self> {
        let data_field = DataField::new("explain", DataType::String);
        let schema = DataSchemaRefExt::create(vec![data_field]);
        Ok(ExplainInterpreter {
            ctx,
            schema,
            plan,
            kind,
        })
    }

    pub fn explain_plan(&self, plan: &Plan) -> Result<Vec<DataBlock>> {
        let result = plan.format_indent()?;
        let line_split_result: Vec<&str> = result.lines().collect();
        let formatted_plan = StringType::from_data(line_split_result);
        Ok(vec![DataBlock::new_from_columns(vec![formatted_plan])])
    }

    pub fn explain_physical_plan(
        &self,
        plan: &PhysicalPlan,
        metadata: &MetadataRef,
    ) -> Result<Vec<DataBlock>> {
        let result = plan
            .format(metadata.clone(), ProfSpanSetRef::default())?
            .format_pretty()?;
        let line_split_result: Vec<&str> = result.lines().collect();
        let formatted_plan = StringType::from_data(line_split_result);
        Ok(vec![DataBlock::new_from_columns(vec![formatted_plan])])
    }

    pub fn explain_join_order(
        &self,
        plan: &PhysicalPlan,
        metadata: &MetadataRef,
    ) -> Result<Vec<DataBlock>> {
        let result = plan.format_join(metadata)?.format_pretty()?;
        let line_split_result: Vec<&str> = result.lines().collect();
        let formatted_plan = StringType::from_data(line_split_result);
        Ok(vec![DataBlock::new_from_columns(vec![formatted_plan])])
    }

    #[async_backtrace::framed]
    pub async fn explain_pipeline(
        &self,
        s_expr: SExpr,
        metadata: MetadataRef,
        ignore_result: bool,
    ) -> Result<Vec<DataBlock>> {
        let mut builder = PhysicalPlanBuilder::new(metadata, self.ctx.clone());
        let plan = builder.build(&s_expr).await?;
        let build_res = build_query_pipeline(&self.ctx, &[], &plan, ignore_result, false).await?;

        let mut blocks = Vec::with_capacity(1 + build_res.sources_pipelines.len());
        // Format root pipeline
        let line_split_result = format!("{}", build_res.main_pipeline.display_indent())
            .lines()
            .map(|s| s.as_bytes().to_vec())
            .collect::<Vec<_>>();
        let column = StringType::from_data(line_split_result);
        blocks.push(DataBlock::new_from_columns(vec![column]));
        // Format child pipelines
        for pipeline in build_res.sources_pipelines.iter() {
            let line_split_result = format!("\n{}", pipeline.display_indent())
                .lines()
                .map(|s| s.as_bytes().to_vec())
                .collect::<Vec<_>>();
            let column = StringType::from_data(line_split_result);
            blocks.push(DataBlock::new_from_columns(vec![column]));
        }
        Ok(blocks)
    }

    #[async_backtrace::framed]
    async fn explain_fragments(
        &self,
        s_expr: SExpr,
        metadata: MetadataRef,
    ) -> Result<Vec<DataBlock>> {
        let ctx = self.ctx.clone();
        let plan = PhysicalPlanBuilder::new(metadata.clone(), self.ctx.clone())
            .build(&s_expr)
            .await?;

        let root_fragment = Fragmenter::try_create(ctx.clone())?.build_fragment(&plan)?;

        let mut fragments_actions = QueryFragmentsActions::create(ctx.clone());
        root_fragment.get_actions(ctx, &mut fragments_actions)?;

        let display_string = fragments_actions.display_indent(&metadata).to_string();
        let line_split_result = display_string
            .lines()
            .map(|s| s.as_bytes().to_vec())
            .collect::<Vec<_>>();
        let formatted_plan = StringType::from_data(line_split_result);
        Ok(vec![DataBlock::new_from_columns(vec![formatted_plan])])
    }

    #[async_backtrace::framed]
    async fn explain_analyze(
        &self,
        s_expr: &SExpr,
        metadata: &MetadataRef,
        ignore_result: bool,
    ) -> Result<Vec<DataBlock>> {
        let mut builder = PhysicalPlanBuilder::new(metadata.clone(), self.ctx.clone());
        let plan = builder.build(s_expr).await?;
        let mut build_res =
            build_query_pipeline(&self.ctx, &[], &plan, ignore_result, true).await?;

        let prof_span_set = build_res.prof_span_set.clone();

        let settings = self.ctx.get_settings();
        let query_id = self.ctx.get_id();
        build_res.set_max_threads(settings.get_max_threads()? as usize);
        let settings = ExecutorSettings::try_create(&settings, query_id)?;

        // Drain the data
        if build_res.main_pipeline.is_complete_pipeline()? {
            let mut pipelines = build_res.sources_pipelines;
            pipelines.push(build_res.main_pipeline);

            let complete_executor = PipelineCompleteExecutor::from_pipelines(pipelines, settings)?;
            complete_executor.execute()?;
        } else {
            let mut pulling_executor =
                PipelinePullingExecutor::from_pipelines(build_res, settings)?;
            pulling_executor.start();
            while (pulling_executor.pull_data()?).is_some() {}
        }

        let result = plan
            .format(metadata.clone(), prof_span_set)?
            .format_pretty()?;
        let line_split_result: Vec<&str> = result.lines().collect();
        let formatted_plan = StringType::from_data(line_split_result);
        Ok(vec![DataBlock::new_from_columns(vec![formatted_plan])])
    }
}
