// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use async_std::stream::Stream;
use std::task::{Context, Poll};

use crate::datablocks::DataBlock;
use crate::datavalues::DataSchemaRef;
use crate::error::Result;

pub struct DataBlockStream {
    index: usize,
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
            index: 0,
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
        Poll::Ready(if self.index < self.data.len() {
            self.index += 1;
            let block = &self.data[self.index - 1];
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
