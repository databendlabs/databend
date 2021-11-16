// Copyright 2020 Datafuse Labs.
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

use std::io::Cursor;
use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use common_functions::scalars::Function;
use common_functions::scalars::FunctionFactory;
use common_planners::InsertIntoPlan;
use common_streams::CorrectWithSchemaStream;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;
use common_streams::SourceStream;
use common_streams::ValueSource;
use tokio_stream::StreamExt;

use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::sessions::DatabendQueryContextRef;

pub struct InsertIntoInterpreter {
    ctx: DatabendQueryContextRef,
    plan: InsertIntoPlan,
    select: Option<Arc<dyn Interpreter>>,
}

impl InsertIntoInterpreter {
    pub fn try_create(
        ctx: DatabendQueryContextRef,
        plan: InsertIntoPlan,
        select: Option<Arc<dyn Interpreter>>,
    ) -> Result<InterpreterPtr> {
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
        let table = self
            .ctx
            .get_table(&self.plan.db_name, &self.plan.tbl_name)?;

        let io_ctx = self.ctx.get_cluster_table_io_context()?;
        let io_ctx = Arc::new(io_ctx);
        let input_stream = if self.plan.values_opt.is_some() {
            let values = self.plan.values_opt.clone().take().unwrap();
            let block_size = self.ctx.get_settings().get_max_block_size()? as usize;
            let values_source =
                ValueSource::new(Cursor::new(values), self.plan.schema(), block_size);
            let stream_source = SourceStream::new(Box::new(values_source));
            stream_source.execute().await
        } else if let Some(select_executor) = &self.select {
            let output_schema = self.plan.schema();
            let select_schema = select_executor.schema();
            if select_schema.fields().len() < output_schema.fields().len() {
                return Err(ErrorCode::BadArguments("Fields in select statement is less than expected"));
            }

            let select_input_stream = select_executor.execute(None).await?;
            let cast_input_stream = select_input_stream.map(move |data_block| match data_block {
                Err(fail) => Err(fail),
                Ok(data_block) => {
                    let rows = data_block.num_rows();
                    let iter = output_schema.fields().iter().zip(select_schema.fields());
                    let mut colunm_vec = vec![];
                    for (i, (output_field, input_field)) in iter.enumerate() {
                        let func = cast_function(output_field.data_type())?;
                        let column = DataColumnWithField::new(
                            data_block.column(i).clone(),
                            input_field.clone(),
                        );
                        let column = func.eval(&[column], rows)?;
                        colunm_vec.push(column);
                    }

                    Ok(DataBlock::create(output_schema.clone(), colunm_vec))
                }
            });

            let stream: SendableDataBlockStream = Box::pin(CorrectWithSchemaStream::new(
                Box::pin(cast_input_stream),
                self.plan.schema.clone(),
            ));
            Ok(stream)
        } else {
            input_stream
                .take()
                .ok_or_else(|| ErrorCode::EmptyData("input stream not exist or consumed"))
        }?;

        table
            .append_data(io_ctx, self.plan.clone(), input_stream)
            .await?;
        Ok(Box::pin(DataBlockStream::create(
            self.plan.schema(),
            None,
            vec![],
        )))
    }
}

fn cast_function(output_type: &DataType) -> Result<Box<dyn Function>> {
    let function_factory = FunctionFactory::instance();
    match output_type {
        DataType::Null => function_factory.get("toNull"),
        DataType::Boolean => function_factory.get("toBoolean"),
        DataType::UInt8 => function_factory.get("toUInt8"),
        DataType::UInt16 => function_factory.get("toUInt16"),
        DataType::UInt32 => function_factory.get("toUInt32"),
        DataType::UInt64 => function_factory.get("toUInt64"),
        DataType::Int8 => function_factory.get("toInt8"),
        DataType::Int16 => function_factory.get("toInt16"),
        DataType::Int32 => function_factory.get("toInt32"),
        DataType::Int64 => function_factory.get("toInt64"),
        DataType::Float32 => function_factory.get("toFloat32"),
        DataType::Float64 => function_factory.get("toFloat64"),
        DataType::Date16 => function_factory.get("toDate16"),
        DataType::Date32 => function_factory.get("toDate32"),
        DataType::String => function_factory.get("toString"),
        DataType::DateTime32(_) => function_factory.get("toDateTime"),
        _ => Err(ErrorCode::BadDataValueType(format!(
            "Unsupported cast operation {:?} for insert into select statment",
            output_type
        ))),
    }
}
