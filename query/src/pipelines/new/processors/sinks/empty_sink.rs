// Copyright 2022 Datafuse Labs.
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

use common_datablocks::DataBlock;
use common_exception::Result;

use super::Sink;
use super::Sinker;
use crate::pipelines::new::processors::port::InputPort;
use crate::pipelines::new::processors::processor::ProcessorPtr;

pub struct EmptySink;

impl EmptySink {
    pub fn create(input: Arc<InputPort>) -> ProcessorPtr {
        Sinker::create(input, EmptySink {})
    }
}

impl Sink for EmptySink {
    const NAME: &'static str = "EmptySink";

    fn consume(&mut self, _: DataBlock) -> Result<()> {
        Ok(())
    }
}
