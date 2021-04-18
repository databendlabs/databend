// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::task::Context;
use std::task::Poll;

use anyhow::Result;
use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;
use common_progress::Progress;
use common_progress::ProgressCallback;
use common_progress::ProgressRef;

use crate::IStream;

pub struct DataBlockStream {
    current: usize,
    schema: DataSchemaRef,
    data: Vec<DataBlock>,
    projects: Option<Vec<usize>>,
    progress: Option<ProgressRef>,
    progress_callback: Option<ProgressCallback>,
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
            progress: None,
            progress_callback: None,
        }
    }
}

impl IStream for DataBlockStream {
    fn set_progress_callback(&mut self, callback: ProgressCallback) {
        self.progress = Some(Progress::create());
        self.progress_callback = Some(callback);
    }
}

impl futures::Stream for DataBlockStream {
    type Item = Result<DataBlock>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        Poll::Ready(if self.current < self.data.len() {
            self.current += 1;
            let block = &self.data[self.current - 1];

            // Progress.
            if let (Some(progress), Some(callback)) = (&self.progress, self.progress_callback) {
                progress.add_rows(block.num_rows());
                progress.add_bytes(block.memory_size());
                (callback)(progress);
            }

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
