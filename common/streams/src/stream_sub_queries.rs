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
use std::task::Context;

use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::Result;
use futures::task::Poll;
use futures::Stream;
use futures::StreamExt;

use crate::SendableDataBlockStream;

pub struct SubQueriesStream {
    input: SendableDataBlockStream,
    schema: DataSchemaRef,
    sub_queries_columns: Vec<DataValue>,
}

impl SubQueriesStream {
    pub fn create(
        schema: DataSchemaRef,
        input: SendableDataBlockStream,
        sub_queries_columns: Vec<DataValue>,
    ) -> SubQueriesStream {
        SubQueriesStream {
            input,
            schema,
            sub_queries_columns,
        }
    }
}

impl Stream for SubQueriesStream {
    type Item = Result<DataBlock>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.input.poll_next_unpin(cx).map(|x| match x {
            Some(Ok(ref block)) => {
                let mut new_columns = block.columns().to_vec();

                let start_index = self.schema.fields().len() - self.sub_queries_columns.len();

                for (index, datavalue) in self.sub_queries_columns.iter().enumerate() {
                    let data_type = self.schema.field(start_index + index).data_type();
                    let col = data_type
                        .create_constant_column(datavalue, block.num_rows())
                        .unwrap();
                    new_columns.push(col);
                }

                Some(Ok(DataBlock::create(self.schema.clone(), new_columns)))
            }
            other => other,
        })
    }
}
