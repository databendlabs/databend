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
use common_datavalues::DataSchemaRef;
use common_exception::Result;

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

impl futures::Stream for DataBlockStream {
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
