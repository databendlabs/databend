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

use std::task::Context;
use std::task::Poll;

use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::Result;
use common_functions::scalars::Function;
use futures::Stream;
use futures::StreamExt;

use crate::SendableDataBlockStream;

pub struct CastStream {
    input: SendableDataBlockStream,
    output_schema: DataSchemaRef,
    functions: Vec<Box<dyn Function>>,
}

impl CastStream {
    pub fn try_create(
        input: SendableDataBlockStream,
        output_schema: DataSchemaRef,
        functions: Vec<Box<dyn Function>>,
    ) -> Result<Self> {
        Ok(CastStream {
            input,
            output_schema,
            functions,
        })
    }

    fn cast(&self, data_block: &DataBlock) -> Result<DataBlock> {
        let rows = data_block.num_rows();
        let iter = self
            .functions
            .iter()
            .zip(data_block.schema().fields())
            .zip(data_block.columns());
        let mut columns = Vec::with_capacity(data_block.num_columns());
        for ((cast_func, input_field), column) in iter {
            let column = ColumnWithField::new(column.clone(), input_field.clone());
            columns.push(cast_func.eval(&[column], rows)?);
        }

        Ok(DataBlock::create(self.output_schema.clone(), columns))
    }
}

impl Stream for CastStream {
    type Item = Result<DataBlock>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        ctx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.input.poll_next_unpin(ctx).map(|x| match x {
            Some(Ok(ref v)) => Some(self.cast(v)),
            other => other,
        })
    }
}
