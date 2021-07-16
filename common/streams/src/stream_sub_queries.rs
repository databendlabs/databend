// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::pin::Pin;
use std::task::Context;

use common_datablocks::DataBlock;
use common_datavalues::columns::DataColumn;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataValue;
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
                for index in 0..self.sub_queries_columns.len() {
                    let values = self.sub_queries_columns[index].clone();
                    new_columns.push(DataColumn::Constant(values, block.num_rows()));
                }

                Some(Ok(DataBlock::create(self.schema.clone(), new_columns)))
            }
            other => other,
        })
    }
}
