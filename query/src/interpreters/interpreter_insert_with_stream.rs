//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datavalues::prelude::Series;
use common_datavalues::prelude::SeriesFrom;
use common_datavalues::DataField;
use common_datavalues::DataSchema;
use common_datavalues::DataSchemaRefExt;
use common_datavalues::DataType;
use common_exception::Result;
use common_planners::Expression;
use common_streams::ProgressStream;
use common_streams::SendableDataBlockStream;

use crate::pipelines::transforms::ExpressionExecutor;
use crate::sessions::QueryContext;
use crate::storages::Table;

pub struct InsertWithStream<'a> {
    ctx: &'a Arc<QueryContext>,
    table: &'a Arc<dyn Table>,
}

impl<'a> InsertWithStream<'a> {
    pub fn new(ctx: &'a Arc<QueryContext>, table: &'a Arc<dyn Table>) -> Self {
        Self { ctx, table }
    }

    pub async fn append_stream(
        &self,
        input: SendableDataBlockStream,
    ) -> common_exception::Result<SendableDataBlockStream> {
        let progress_stream = Box::pin(ProgressStream::try_create(
            input,
            self.ctx.get_scan_progress(),
        )?);
        self.table
            .append_data(self.ctx.clone(), progress_stream)
            .await
    }
}

pub trait SendableWithSchema {
    fn to_stream(self, schema: Arc<DataSchema>) -> Result<SendableDataBlockStream>;
}

impl SendableWithSchema for &[Vec<Expression>] {
    fn to_stream(self, schema: Arc<DataSchema>) -> Result<SendableDataBlockStream> {
        let dummy = DataSchemaRefExt::create(vec![DataField::new("dummy", DataType::UInt8, false)]);
        let one_row_block = DataBlock::create_by_array(dummy.clone(), vec![Series::new(vec![1u8])]);
        let blocks = self
            .as_ref()
            .iter()
            .map(|exprs| {
                let executor = ExpressionExecutor::try_create(
                    "Insert into from values",
                    dummy.clone(),
                    schema.clone(),
                    exprs.clone(),
                    true,
                )?;
                executor.execute(&one_row_block)
            })
            .collect::<common_exception::Result<Vec<_>>>()?;

        let stream = Box::pin(futures::stream::iter(vec![DataBlock::concat_blocks(
            &blocks,
        )]));
        Ok(stream)
    }
}
