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
use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;

use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::sessions::QueryContext;
use crate::sql::exec::PipelineBuilder;
use crate::sql::optimizer::SExpr;
use crate::sql::plans::Plan;
use crate::sql::BindContext;
use crate::sql::MetadataRef;

pub struct ExplainInterpreterV2 {
    ctx: Arc<QueryContext>,
    schema: DataSchemaRef,
    kind: ExplainKind,
    plan: Plan,
}

#[async_trait::async_trait]
impl Interpreter for ExplainInterpreterV2 {
    fn name(&self) -> &str {
        "ExplainInterpreterV2"
    }

    async fn execute(
        &self,
        _input_stream: Option<SendableDataBlockStream>,
    ) -> Result<SendableDataBlockStream> {
        let blocks = match &self.kind {
            ExplainKind::Syntax => self.explain_syntax(&self.plan).await?,
            ExplainKind::Pipeline => match &self.plan {
                Plan::Query {
                    s_expr,
                    metadata,
                    bind_context,
                } => {
                    self.explain_pipeline(s_expr.clone(), *bind_context.clone(), metadata.clone())
                        .await?
                }
                _ => {
                    return Err(ErrorCode::UnImplement("Unsupported EXPLAIN statement"));
                }
            },
            ExplainKind::Graph => {
                return Err(ErrorCode::UnImplement("ExplainKind graph is unimplemented"));
            }
        };
        Ok(Box::pin(DataBlockStream::create(
            self.schema.clone(),
            None,
            blocks,
        )))
    }

    async fn start(&self) -> Result<()> {
        Ok(())
    }

    async fn finish(&self) -> Result<()> {
        Ok(())
    }
}

impl ExplainInterpreterV2 {
    pub fn try_create(
        ctx: Arc<QueryContext>,
        plan: Plan,
        kind: ExplainKind,
    ) -> Result<InterpreterPtr> {
        let data_field = DataField::new("explain", DataTypeImpl::String(StringType::default()));
        let schema = DataSchemaRefExt::create(vec![data_field]);
        Ok(Arc::new(ExplainInterpreterV2 {
            ctx,
            schema,
            plan,
            kind,
        }))
    }

    pub async fn explain_syntax(&self, plan: &Plan) -> Result<Vec<DataBlock>> {
        let result = plan.format_indent()?;
        let line_splitted_result: Vec<&str> = result.lines().collect();
        let formatted_plan = Series::from_data(line_splitted_result);
        Ok(vec![DataBlock::create(self.schema.clone(), vec![
            formatted_plan,
        ])])
    }

    pub async fn explain_pipeline(
        &self,
        s_expr: SExpr,
        bind_context: BindContext,
        metadata: MetadataRef,
    ) -> Result<Vec<DataBlock>> {
        let pb = PipelineBuilder::new(
            self.ctx.clone(),
            bind_context.result_columns(),
            metadata,
            s_expr,
        );
        let (root_pipeline, pipelines, _) = pb.spawn()?;
        let mut blocks = vec![];
        // Format root pipeline
        blocks.push(DataBlock::create(self.schema.clone(), vec![
            Series::from_data(
                format!("{:?}", root_pipeline)
                    .lines()
                    .map(|s| s.as_bytes())
                    .collect::<Vec<_>>(),
            ),
        ]));
        // Format child pipelines
        for pipeline in pipelines.iter() {
            blocks.push(DataBlock::create(self.schema.clone(), vec![
                Series::from_data(
                    format!("\n{:?}", pipeline)
                        .lines()
                        .map(|s| s.as_bytes())
                        .collect::<Vec<_>>(),
                ),
            ]));
        }
        Ok(blocks)
    }
}
