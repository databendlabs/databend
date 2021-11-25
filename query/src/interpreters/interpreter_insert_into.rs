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
use common_datavalues::DataSchemaRefExt;
use common_datavalues::DataType;
use common_exception::ErrorCode;
use common_exception::Result;
use common_functions::scalars::CastFunction;
use common_planners::InsertIntoPlan;
use common_planners::PlanNode;
use common_streams::CastStream;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;

use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::interpreters::SelectInterpreter;
use crate::pipelines::transforms::ExpressionExecutor;
use crate::sessions::QueryContext;

pub struct InsertIntoInterpreter {
    ctx: Arc<QueryContext>,
    plan: InsertIntoPlan,
    select: Option<Arc<dyn Interpreter>>,
}

impl InsertIntoInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: InsertIntoPlan) -> Result<InterpreterPtr> {
        let select = match plan.select_plan.clone().take() {
            Some(select_plan) => {
                if let PlanNode::Select(select_plan_node) = *select_plan {
                    Ok(Some(SelectInterpreter::try_create(
                        ctx.clone(),
                        select_plan_node,
                    )?))
                } else {
                    Result::Err(ErrorCode::UnknownTypeOfQuery(format!(
                        "Unsupported select query plan for insert_into interpreter:{}",
                        select_plan.name()
                    )))
                }
            }
            None => Ok(None),
        }?;
        Ok(Arc::new(InsertIntoInterpreter { ctx, plan, select }))
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
        let database = &self.plan.db_name;
        let table = &self.plan.tbl_name;
        let write_table = self.ctx.get_table(database, table).await?;

        let input_stream = if self.plan.value_exprs_opt.is_some() {
            let values_exprs = self.plan.value_exprs_opt.clone().take().unwrap();
            let dummy =
                DataSchemaRefExt::create(vec![DataField::new("dummy", DataType::UInt8, false)]);
            let one_row_block =
                DataBlock::create_by_array(dummy.clone(), vec![Series::new(vec![1u8])]);

            let blocks = values_exprs
                .iter()
                .map(|exprs| {
                    let executor = ExpressionExecutor::try_create(
                        "Insert into from values",
                        dummy.clone(),
                        self.plan.schema(),
                        exprs.clone(),
                        true,
                    )?;
                    executor.execute(&one_row_block)
                })
                .collect::<Result<Vec<_>>>()?;
            // merge into one block in sync mode
            let stream: SendableDataBlockStream =
                Box::pin(futures::stream::iter(vec![DataBlock::concat_blocks(
                    &blocks,
                )]));

            Ok(stream)
        } else if let Some(select_executor) = &self.select {
            let output_schema = self.plan.schema();
            let select_schema = select_executor.schema();
            if select_schema.fields().len() < output_schema.fields().len() {
                return Err(ErrorCode::BadArguments(
                    "Fields in select statement is less than expected",
                ));
            }

            let mut functions = Vec::with_capacity(output_schema.fields().len());
            for field in output_schema.fields() {
                let cast_function =
                    CastFunction::create("cast".to_string(), field.data_type().clone())?;
                functions.push(cast_function);
            }
            let stream: SendableDataBlockStream = Box::pin(CastStream::try_create(
                select_executor.execute(None).await?,
                output_schema,
                functions,
            )?);
            Ok(stream)
        } else {
            input_stream
                .take()
                .ok_or_else(|| ErrorCode::EmptyData("input stream not exist or consumed"))
        }?;

        write_table
            .append_data(self.ctx.clone(), self.plan.clone(), input_stream)
            .await?;
        Ok(Box::pin(DataBlockStream::create(
            self.plan.schema(),
            None,
            vec![],
        )))
    }
}
