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

use common_datablocks::DataBlock;
use common_datavalues::series::Series;
use common_datavalues::series::SeriesFrom;
use common_datavalues::DataField;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataSchemaRefExt;
use common_datavalues::DataType;
use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::Expression;
use common_planners::InputSource;
use common_planners::InsertIntoPlan;
use common_streams::DataBlockStream;
use common_streams::ProgressStream;
use common_streams::SendableDataBlockStream;
use futures::TryStreamExt;

use crate::interpreters::interpreter_insert_into_with_plan::InsertIntoWithPlan;
use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::pipelines::transforms::ExpressionExecutor;
use crate::sessions::QueryContext;
use crate::storages::Table;

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
        input_stream: Option<SendableDataBlockStream>,
    ) -> Result<SendableDataBlockStream> {
        let database = &self.plan.database_name;
        let table = &self.plan.table_name;
        let table = self.ctx.get_table(database, table).await?;
        let append_op_logs = match &self.plan.source {
            InputSource::SelectPlan(plan_node) => {
                let executor = InsertIntoWithPlan {
                    ctx: &self.ctx,
                    schema: &self.plan.schema,
                };
                executor.execute(plan_node, table.as_ref()).await
            }
            InputSource::Expressions(values_exprs) => {
                let exec = StreamExec {
                    ctx: &self.ctx,
                    schema: &self.plan.schema,
                    table: &table,
                };
                exec.append_value_exprs(values_exprs).await
            }
            InputSource::StreamingWithFormat(_) => {
                let exec = StreamExec {
                    ctx: &self.ctx,
                    schema: &self.plan.schema,
                    table: &table,
                };
                exec.append_stream(input_stream).await
            }
        }?;

        // feed back the append operation logs to table
        table
            .commit(
                self.ctx.clone(),
                append_op_logs.try_collect().await?,
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

struct StreamExec<'a> {
    ctx: &'a Arc<QueryContext>,
    table: &'a Arc<dyn Table>,
    schema: &'a DataSchemaRef,
}

impl<'a> StreamExec<'a> {
    fn block_from_values_exprs(&self, values_exprs: &[Vec<Expression>]) -> Result<Vec<DataBlock>> {
        let dummy = DataSchemaRefExt::create(vec![DataField::new("dummy", DataType::UInt8, false)]);
        let one_row_block = DataBlock::create_by_array(dummy.clone(), vec![Series::new(vec![1u8])]);
        values_exprs
            .iter()
            .map(|exprs| {
                let executor = ExpressionExecutor::try_create(
                    "Insert into from values",
                    dummy.clone(),
                    self.schema.clone(),
                    exprs.clone(),
                    true,
                )?;
                executor.execute(&one_row_block)
            })
            .collect::<Result<Vec<_>>>()
    }

    async fn do_append(
        &self,
        input_stream: SendableDataBlockStream,
    ) -> Result<SendableDataBlockStream> {
        let progress_stream = Box::pin(ProgressStream::try_create(
            input_stream,
            self.ctx.progress_callback()?,
        )?);
        self.table
            .append_data(self.ctx.clone(), progress_stream)
            .await
    }
    async fn append_value_exprs(
        &self,
        values_exprs: &[Vec<Expression>],
    ) -> Result<SendableDataBlockStream> {
        let blocks = self.block_from_values_exprs(values_exprs)?;
        let stream: SendableDataBlockStream =
            Box::pin(futures::stream::iter(vec![DataBlock::concat_blocks(
                &blocks,
            )]));
        self.do_append(stream).await
    }

    async fn append_stream(
        &self,
        mut input_stream: Option<SendableDataBlockStream>,
    ) -> Result<SendableDataBlockStream> {
        let stream = input_stream
            .take()
            .ok_or_else(|| ErrorCode::EmptyData("input stream not exist or consumed"))?;
        self.do_append(stream).await
    }
}
