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

use common_exception::ErrorCode;
use common_exception::Result;
use common_functions::scalars::CastFunction;
use common_planners::InsertIntoPlan;
use common_streams::CastStream;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;
use common_streams::SourceStream;
use common_streams::ValueSource;

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
