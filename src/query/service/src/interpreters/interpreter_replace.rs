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

use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::DataSchemaRef;
use common_pipeline_sources::AsyncSourcer;
use common_sql::plans::InsertInputSource;
use common_sql::plans::Replace;
use common_sql::NameResolutionContext;

use crate::interpreters::interpreter_insert::ValueSource;
use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;

#[allow(dead_code)]
pub struct ReplaceInterpreter {
    ctx: Arc<QueryContext>,
    plan: Replace,
}

impl ReplaceInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: Replace) -> Result<InterpreterPtr> {
        Ok(Arc::new(ReplaceInterpreter { ctx, plan }))
    }
}

#[async_trait::async_trait]
impl Interpreter for ReplaceInterpreter {
    fn name(&self) -> &str {
        "ReplaceIntoInterpreter"
    }

    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let mut pipeline = connect_input_source(&self.ctx, &self.plan.source, self.plan.schema())?;

        let plan = &self.plan;
        let table = self
            .ctx
            .get_table(&plan.catalog, &plan.database, &plan.table)
            .await?;
        let field = plan.join_on.clone();
        table
            .replace_into(self.ctx.clone(), &mut pipeline.main_pipeline, field)
            .await?;
        Ok(pipeline)
    }
}

fn connect_input_source(
    ctx: &Arc<QueryContext>,
    source: &InsertInputSource,
    schema: DataSchemaRef,
) -> Result<PipelineBuildResult> {
    let mut build_res = PipelineBuildResult::create();

    match source {
        InsertInputSource::Values(data) => {
            let settings = ctx.get_settings();

            build_res.main_pipeline.add_source(
                |output| {
                    let name_resolution_ctx = NameResolutionContext::try_from(settings.as_ref())?;
                    let inner = ValueSource::new(
                        data.to_string(),
                        ctx.clone(),
                        name_resolution_ctx,
                        schema.clone(),
                    );
                    AsyncSourcer::create(ctx.clone(), output, inner)
                },
                1,
            )?;
        }
        _ => {
            todo!()
        }
    }
    Ok(build_res)
}
