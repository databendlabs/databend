// Copyright 2021 Datafuse Labs
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

use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use databend_common_base::base::Progress;
use databend_common_base::base::ProgressValues;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::SendableDataBlockStream;
use futures::Stream;
use pin_project_lite::pin_project;

pin_project! {
    pub struct ProgressStream {
        #[pin]
        input: SendableDataBlockStream,
        progress:Arc<Progress>,
    }
}

impl ProgressStream {
    pub fn try_create(input: SendableDataBlockStream, progress: Arc<Progress>) -> Result<Self> {
        Ok(Self { input, progress })
    }
}

impl Stream for ProgressStream {
    type Item = Result<DataBlock>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        ctx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = self.project();

        match this.input.poll_next(ctx) {
            Poll::Ready(x) => match x {
                Some(result) => match result {
                    Ok(block) => {
                        let progress_values = ProgressValues {
                            rows: block.num_rows(),
                            bytes: block.memory_size(),
                        };
                        this.progress.incr(&progress_values);
                        Poll::Ready(Some(Ok(block)))
                    }
                    Err(e) => Poll::Ready(Some(Err(e))),
                },
                None => Poll::Ready(None),
            },
            Poll::Pending => Poll::Pending,
        }
    }
}
