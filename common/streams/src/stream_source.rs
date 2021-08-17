// Copyright 2020 Datafuse Labs.
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
use common_exception::Result;
use pin_project_lite::pin_project;

use crate::Source;

pin_project! {
    pub struct SourceStream {
        source: Box<dyn Source>,
    }
}

impl SourceStream {
    pub fn create(source: Box<dyn Source>) -> Self {
        SourceStream { source }
    }
}

impl futures::Stream for SourceStream {
    type Item = Result<DataBlock>;

    fn poll_next(self: std::pin::Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        let block = this.source.read()?;
        Poll::Ready(block.map(Ok))
    }
}
