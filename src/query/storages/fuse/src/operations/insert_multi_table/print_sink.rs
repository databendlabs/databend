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

use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_sinks::Sink;
use databend_common_pipeline_sinks::Sinker;
pub struct PrintSink;

impl PrintSink {
    pub fn create(input: Arc<InputPort>) -> Box<dyn Processor> {
        Sinker::create(input, PrintSink {})
    }
}

impl Sink for PrintSink {
    const NAME: &'static str = "PrintSink";

    fn consume(&mut self, block: DataBlock) -> Result<()> {
        println!("{:?}", block);
        Ok(())
    }
}
