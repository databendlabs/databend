// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::task::Context;
use std::task::Poll;

use anyhow::Result;
use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;
use futures::stream::Stream;

pub struct DataBlockStream {
    current: usize,
    schema: DataSchemaRef,
    data: Vec<DataBlock>,
    projects: Option<Vec<usize>>,
}

impl DataBlockStream {
    pub fn create(
        schema: DataSchemaRef,
        projects: Option<Vec<usize>>,
        data: Vec<DataBlock>,
    ) -> Self {
        DataBlockStream {
            current: 0,
            schema,
            data,
            projects,
        }
    }
}

impl Stream for DataBlockStream {
    type Item = Result<DataBlock>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        Poll::Ready(if self.current < self.data.len() {
            self.current += 1;
            let block = &self.data[self.current - 1];
            Some(Ok(match &self.projects {
                Some(v) => DataBlock::create(
                    self.schema.clone(),
                    v.iter().map(|x| block.column(*x).clone()).collect(),
                ),
                None => block.clone(),
            }))
        } else {
            None
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.data.len(), Some(self.data.len()))
    }
}
