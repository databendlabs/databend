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

use async_stream::stream;
use common_exception::Result;

use crate::SendableDataBlockStream;
use crate::Source;

pub struct SourceStream {
    source: Box<dyn Source>,
}

impl SourceStream {
    pub fn new(source: Box<dyn Source>) -> Self {
        SourceStream { source }
    }

    pub async fn execute(self) -> Result<SendableDataBlockStream> {
        let mut source = self.source;
        let s = stream! {
            loop {
                let block =  source.read().await;
                match block {
                    Ok(None) => break,
                    Ok(Some(b)) =>  yield(Ok(b)),
                    Err(e) => yield(Err(e)),
                }
            }
        };
        Ok(Box::pin(s))
    }
}
