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

use std::any::Any;
use std::sync::Arc;

use common_datavalues::DataSchemaRef;
use common_exception::Result;
use common_functions::scalars::CastFunction;
use common_meta_types::TableInfo;
use common_streams::CastStream;
use common_streams::SendableDataBlockStream;
use common_tracing::tracing;

use crate::catalogs::Catalog;
use crate::pipelines::processors::EmptyProcessor;
use crate::pipelines::processors::Processor;
use crate::pipelines::transforms::AddOnStream;
use crate::sessions::QueryContext;

pub struct SinkTransform {
    ctx: Arc<QueryContext>,
    table_info: TableInfo,
    input: Arc<dyn Processor>,
    cast_schema: Option<DataSchemaRef>,
    input_schema: DataSchemaRef,
}

impl SinkTransform {
    pub fn create(
        ctx: Arc<QueryContext>,
        table_info: TableInfo,
        cast_schema: Option<DataSchemaRef>,
        input_schema: DataSchemaRef,
    ) -> Self {
        Self {
            ctx,
            table_info,
            input: Arc::new(EmptyProcessor::create()),
            cast_schema,
            input_schema,
        }
    }
    fn table_info(&self) -> &TableInfo {
        &self.table_info
    }
}

#[async_trait::async_trait]
impl Processor for SinkTransform {
    fn name(&self) -> &str {
        "SinkTransform"
    }

    fn connect_to(&mut self, input: Arc<dyn Processor>) -> Result<()> {
        self.input = input;
        Ok(())
    }

    fn inputs(&self) -> Vec<Arc<dyn Processor>> {
        vec![self.input.clone()]
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    #[tracing::instrument(level = "debug", name = "sink_execute", skip(self))]
    async fn execute(&self) -> Result<SendableDataBlockStream> {
        tracing::debug!("executing sinks transform");
        let tbl = self
            .ctx
            .get_catalog()
            .get_table_by_info(self.table_info())?;
        let mut input_stream = self.input.execute().await?;

        if let Some(cast_schema) = &self.cast_schema {
            let mut functions = Vec::with_capacity(cast_schema.fields().len());
            for field in cast_schema.fields() {
                let name = format!("{:?}", field.data_type());
                let cast_function = CastFunction::create("cast", &name).unwrap();
                functions.push(cast_function);
            }
            input_stream = Box::pin(CastStream::try_create(
                input_stream,
                cast_schema.clone(),
                functions,
            )?);
        }

        let input_schema = self.input_schema.clone();
        let output_schema = self.table_info.schema();
        if self.input_schema != output_schema {
            input_stream = Box::pin(AddOnStream::try_create(
                input_stream,
                input_schema,
                output_schema,
            )?)
        }

        tbl.append_data(self.ctx.clone(), input_stream).await
    }
}
