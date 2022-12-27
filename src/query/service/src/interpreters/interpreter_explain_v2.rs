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
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::DataType;
use common_expression::Chunk;
use common_expression::Column;
use common_expression::ColumnFrom;
use common_expression::DataField;
use common_expression::DataSchemaRef;
use common_expression::DataSchemaRefExt;
use common_expression::Value;
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
        let chunks = match &self.kind {
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
                let num_rows = line_splitted_result.len();
                let column = Column::from_data(line_splitted_result);
                vec![Chunk::new_from_sequence(
                    vec![(Value::Column(column), DataType::String)],
                    num_rows,
                )]
            }
        };

        PipelineBuildResult::from_chunks(chunks)
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

    pub fn explain_plan(&self, plan: &Plan) -> Result<Vec<Chunk>> {
        let result = plan.format_indent()?;
        let line_splitted_result: Vec<&str> = result.lines().collect();
        let num_rows = line_splitted_result.len();
        let formatted_plan = Column::from_data(line_splitted_result);
        Ok(vec![Chunk::new_from_sequence(
            vec![(Value::Column(formatted_plan), DataType::String)],
            num_rows,
        )])
    }

    pub fn explain_physical_plan(
        &self,
        plan: &PhysicalPlan,
        metadata: &MetadataRef,
    ) -> Result<Vec<Chunk>> {
        let result = plan.format(metadata.clone())?;
        let line_splitted_result: Vec<&str> = result.lines().collect();
        let num_rows = line_splitted_result.len();
        let formatted_plan = Column::from_data(line_splitted_result);
        Ok(vec![Chunk::new_from_sequence(
            vec![(Value::Column(formatted_plan), DataType::String)],
            num_rows,
        )])
    }

    pub async fn explain_pipeline(
        &self,
        s_expr: SExpr,
        metadata: MetadataRef,
        ignore_result: bool,
    ) -> Result<Vec<Chunk>> {
        let builder = PhysicalPlanBuilder::new(metadata, self.ctx.clone());
        let plan = builder.build(&s_expr).await?;
        let build_res = build_query_pipeline(&self.ctx, &[], &plan, ignore_result).await?;

        let mut chunks = Vec::with_capacity(1 + build_res.sources_pipelines.len());
        // Format root pipeline
        let line_splitted_result = format!("{}", build_res.main_pipeline.display_indent())
            .lines()
            .map(|s| s.as_bytes().to_vec())
            .collect::<Vec<_>>();
        let num_rows = line_splitted_result.len();
        let column = Column::from_data(line_splitted_result);
        chunks.push(Chunk::new_from_sequence(
            vec![(Value::Column(column), DataType::String)],
            num_rows,
        ));
        // Format child pipelines
        for pipeline in build_res.sources_pipelines.iter() {
            let line_splitted_result = format!("\n{}", pipeline.display_indent())
                .lines()
                .map(|s| s.as_bytes().to_vec())
                .collect::<Vec<_>>();
            let num_rows = line_splitted_result.len();
            let column = Column::from_data(line_splitted_result);
            chunks.push(Chunk::new_from_sequence(
                vec![(Value::Column(column), DataType::String)],
                num_rows,
            ));
        }
        Ok(chunks)
    }

    async fn explain_fragments(&self, s_expr: SExpr, metadata: MetadataRef) -> Result<Vec<Chunk>> {
        let ctx = self.ctx.clone();
        let plan = PhysicalPlanBuilder::new(metadata, self.ctx.clone())
            .build(&s_expr)
            .await?;

        let root_fragment = Fragmenter::try_create(ctx.clone())?.build_fragment(&plan)?;

        let mut fragments_actions = QueryFragmentsActions::create(ctx.clone());
        root_fragment.get_actions(ctx, &mut fragments_actions)?;

        let display_string = fragments_actions.display_indent().to_string();
        let line_splitted_result = display_string
            .lines()
            .map(|s| s.as_bytes().to_vec())
            .collect::<Vec<_>>();
        let num_rows = line_splitted_result.len();
        let formatted_plan = Column::from_data(line_splitted_result);
        Ok(vec![Chunk::new_from_sequence(
            vec![(Value::Column(formatted_plan), DataType::String)],
            num_rows,
        )])
    }
}
