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

use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_types::GrantObject;
use common_meta_types::UserPrivilegeType;
use common_planners::InsertInputSource;
use common_planners::InsertPlan;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;
use futures::TryStreamExt;

use super::interpreter_insert_with_stream::SendableWithSchema;
use crate::interpreters::interpreter_insert_with_stream::InsertWithStream;
use crate::interpreters::plan_schedulers::InsertWithPlan;
use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::pipelines::transforms::AddOnStream;
use crate::sessions::QueryContext;

pub struct InsertInterpreter {
    ctx: Arc<QueryContext>,
    plan: InsertPlan,
}

impl InsertInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: InsertPlan) -> Result<InterpreterPtr> {
        Ok(Arc::new(InsertInterpreter { ctx, plan }))
    }
}

#[async_trait::async_trait]
impl Interpreter for InsertInterpreter {
    fn name(&self) -> &str {
        "InsertIntoInterpreter"
    }

    async fn execute(
        &self,
        mut input_stream: Option<SendableDataBlockStream>,
    ) -> Result<SendableDataBlockStream> {
        let plan = &self.plan;

        self.ctx
            .get_current_session()
            .validate_privilege(
                &GrantObject::Table(plan.database_name.clone(), plan.table_name.clone()),
                UserPrivilegeType::Insert,
            )
            .await?;

        let table = self
            .ctx
            .get_table(&plan.database_name, &plan.table_name)
            .await?;

        let need_fill_missing_columns = table.schema() != self.plan.schema();

        let append_logs = match &self.plan.source {
            InsertInputSource::SelectPlan(plan_node) => {
                let with_plan = InsertWithPlan::new(&self.ctx, &self.plan.schema, plan_node);
                with_plan.execute(table.as_ref()).await
            }
            InsertInputSource::Expressions(values_exprs) => {
                let stream = values_exprs.to_stream(self.plan.schema.clone())?;
                let stream = if need_fill_missing_columns {
                    Box::pin(AddOnStream::try_create(
                        stream,
                        self.plan.schema(),
                        table.schema(),
                    )?)
                } else {
                    stream
                };

                let with_stream = InsertWithStream::new(&self.ctx, &table);
                with_stream.append_stream(stream).await
            }
            InsertInputSource::StreamingWithFormat(_) => {
                let stream = input_stream
                    .take()
                    .ok_or_else(|| ErrorCode::EmptyData("input stream not exist or consumed"))?;

                let stream = if need_fill_missing_columns {
                    Box::pin(AddOnStream::try_create(
                        stream,
                        self.plan.schema(),
                        table.schema(),
                    )?)
                } else {
                    stream
                };

                let with_stream = InsertWithStream::new(&self.ctx, &table);
                with_stream.append_stream(stream).await
            }
        }?;
        // feed back the append operation logs to table
        table
            .commit_insertion(
                self.ctx.clone(),
                append_logs.try_collect().await?,
                self.plan.overwrite,
            )
            .await?;

        Ok(Box::pin(DataBlockStream::create(
            self.plan.schema(),
            None,
            vec![],
        )))
    }
}
