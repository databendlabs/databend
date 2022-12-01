// Copyright 2021 Datafuse Labs.
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
use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use common_sql::MetadataRef;

use crate::interpreters::Interpreter;
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

    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let blocks = match &self.kind {
            ExplainKind::Raw => self.explain_plan(&self.plan)?,

            ExplainKind::Plan => match &self.plan {
                Plan::Query {
                    s_expr, metadata, ..
                } => {
                    let ctx = self.ctx.clone();
                    let settings = ctx.get_settings();
                    settings.set_enable_distributed_eval_index(false)?;

                    let builder = PhysicalPlanBuilder::new(metadata.clone(), ctx);
                    let plan = builder.build(s_expr).await?;
                    self.explain_physical_plan(&plan, metadata)?
                }
                _ => self.explain_plan(&self.plan)?,
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
                let line_splitted_result: Vec<&str> = display_string.lines().collect();
                let column = Series::from_data(line_splitted_result);
                vec![DataBlock::create(self.schema.clone(), vec![column])]
            }
        };

        PipelineBuildResult::from_blocks(blocks)
    }
}

impl ExplainInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: Plan, kind: ExplainKind) -> Result<Self> {
        let data_field = DataField::new("explain", DataTypeImpl::String(StringType::default()));
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
        let line_splitted_result: Vec<&str> = result.lines().collect();
        let formatted_plan = Series::from_data(line_splitted_result);
        Ok(vec![DataBlock::create(self.schema.clone(), vec![
            formatted_plan,
        ])])
    }

    pub fn explain_physical_plan(
        &self,
        plan: &PhysicalPlan,
        metadata: &MetadataRef,
    ) -> Result<Vec<DataBlock>> {
        let result = plan.format(metadata.clone())?;
        let line_splitted_result: Vec<&str> = result.lines().collect();
        let formatted_plan = Series::from_data(line_splitted_result);
        Ok(vec![DataBlock::create(self.schema.clone(), vec![
            formatted_plan,
        ])])
    }

    pub async fn explain_pipeline(
        &self,
        s_expr: SExpr,
        metadata: MetadataRef,
        ignore_result: bool,
    ) -> Result<Vec<DataBlock>> {
        let builder = PhysicalPlanBuilder::new(metadata, self.ctx.clone());
        let plan = builder.build(&s_expr).await?;
        let build_res = build_query_pipeline(&self.ctx, &[], &plan, ignore_result).await?;

        let mut blocks = vec![];
        // Format root pipeline
        blocks.push(DataBlock::create(self.schema.clone(), vec![
            Series::from_data(
                format!("{}", build_res.main_pipeline.display_indent())
                    .lines()
                    .map(|s| s.as_bytes())
                    .collect::<Vec<_>>(),
            ),
        ]));
        // Format child pipelines
        for pipeline in build_res.sources_pipelines.iter() {
            blocks.push(DataBlock::create(self.schema.clone(), vec![
                Series::from_data(
                    format!("\n{}", pipeline.display_indent())
                        .lines()
                        .map(|s| s.as_bytes())
                        .collect::<Vec<_>>(),
                ),
            ]));
        }
        Ok(blocks)
    }

    async fn explain_fragments(
        &self,
        s_expr: SExpr,
        metadata: MetadataRef,
    ) -> Result<Vec<DataBlock>> {
        let ctx = self.ctx.clone();
        let plan = PhysicalPlanBuilder::new(metadata, self.ctx.clone())
            .build(&s_expr)
            .await?;

        let root_fragment = Fragmenter::try_create(ctx.clone())?.build_fragment(&plan)?;

        let mut fragments_actions = QueryFragmentsActions::create(ctx.clone());
        root_fragment.get_actions(ctx, &mut fragments_actions)?;

        let display_string = fragments_actions.display_indent().to_string();
        let formatted_fragments = Series::from_data(
            display_string
                .lines()
                .map(|s| s.as_bytes())
                .collect::<Vec<_>>(),
        );
        Ok(vec![DataBlock::create(self.schema.clone(), vec![
            formatted_fragments,
        ])])
    }
}
