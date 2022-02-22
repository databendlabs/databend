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

use std::pin::Pin;

use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;
use common_exception::Result;
use futures::task::Context;
use futures::task::Poll;
use futures::Stream;
use futures::StreamExt;

use crate::SendableDataBlockStream;

pub struct CorrectWithSchemaStream {
    input: SendableDataBlockStream,
    schema: DataSchemaRef,
}

impl CorrectWithSchemaStream {
    pub fn new(input: SendableDataBlockStream, schema: DataSchemaRef) -> Self {
        CorrectWithSchemaStream { input, schema }
    }

    fn new_block_if_need(&self, data_block: DataBlock) -> Result<DataBlock> {
        match self.schema.eq(data_block.schema()) {
            true => Ok(data_block),
            false => self.new_data_block(data_block),
        }
    }

    fn new_data_block(&self, data_block: DataBlock) -> Result<DataBlock> {
        let schema_fields = self.schema.fields();
        let mut new_columns = Vec::with_capacity(schema_fields.len());
        for schema_field in schema_fields {
            let column = data_block.try_column_by_name(schema_field.name())?;
            let physical_type = column.data_type_id().to_physical_type();
            let physical_type_expect = schema_field.data_type().data_type_id().to_physical_type();

            if physical_type == physical_type_expect {
                new_columns.push(column.clone())
            }
        }

        Ok(DataBlock::create(self.schema.clone(), new_columns))
    }
}

impl Stream for CorrectWithSchemaStream {
    type Item = Result<DataBlock>;

    fn poll_next(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.input.poll_next_unpin(ctx).map(|x| match x {
            Some(Ok(block)) => Some(self.new_block_if_need(block)),
            other => other,
        })
    }
}
