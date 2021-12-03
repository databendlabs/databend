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
use common_planners::InsertInputSource;
use common_planners::InsertIntoPlan;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;
use futures::TryStreamExt;

use super::interpreter_insert_into_with_stream::SendableWithSchema;
use crate::interpreters::interpreter_insert_into_with_plan::InsertIntoWithPlan;
use crate::interpreters::interpreter_insert_into_with_stream::InsertWithStream;
use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::sessions::QueryContext;

pub struct InsertIntoInterpreter {
    ctx: Arc<QueryContext>,
    plan: InsertIntoPlan,
}

impl InsertIntoInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: InsertIntoPlan) -> Result<InterpreterPtr> {
        Ok(Arc::new(InsertIntoInterpreter { ctx, plan }))
    }
}

#[async_trait::async_trait]
impl Interpreter for InsertIntoInterpreter {
    fn name(&self) -> &str {
        "InsertIntoInterpreter"
    }

    async fn execute(
        &self,
        mut input_stream: Option<SendableDataBlockStream>,
    ) -> Result<SendableDataBlockStream> {
        let plan = &self.plan;
        let table = self
            .ctx
            .get_table(&plan.database_name, &plan.table_name)
            .await?;

        let append_logs = match &self.plan.source {
            InsertInputSource::SelectPlan(plan_node) => {
                let with_plan = InsertIntoWithPlan::new(&self.ctx, &self.plan.schema, plan_node);
                with_plan.execute(table.as_ref()).await
            }
            InsertInputSource::Expressions(values_exprs) => {
                let stream = values_exprs.to_stream(self.plan.schema.clone())?;
                let with_stream = InsertWithStream::new(&self.ctx, &table);
                with_stream.append_stream(stream).await
            }
            InsertInputSource::StreamingWithFormat(_) => {
                let stream = input_stream
                    .take()
                    .ok_or_else(|| ErrorCode::EmptyData("input stream not exist or consumed"))?;
                let with_stream = InsertWithStream::new(&self.ctx, &table);
                with_stream.append_stream(stream).await
            }
        }?;

        // feed back the append operation logs to table
        table
            .commit(
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
