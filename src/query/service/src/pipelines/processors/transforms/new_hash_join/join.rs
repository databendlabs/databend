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

use std::any::Any;

use databend_common_base::base::ProgressValues;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;

use crate::pipelines::processors::transforms::JoinRuntimeFilterPacket;
use crate::pipelines::processors::transforms::RuntimeFiltersDesc;

pub trait JoinStream: Send + Sync {
    fn next(&mut self) -> Result<Option<DataBlock>>;
}

pub trait Join: Any + Send + Sync + 'static {
    fn add_block(&mut self, data: Option<DataBlock>) -> Result<()>;

    fn final_build(&mut self) -> Result<Option<ProgressValues>>;

    fn build_runtime_filter(&self, _: &RuntimeFiltersDesc) -> Result<JoinRuntimeFilterPacket> {
        Ok(JoinRuntimeFilterPacket::default())
    }

    fn probe_block(&mut self, data: DataBlock) -> Result<Box<dyn JoinStream + '_>>;

    fn final_probe(&mut self) -> Result<Option<Box<dyn JoinStream + '_>>> {
        Ok(None)
    }
}

pub struct EmptyJoinStream;

impl JoinStream for EmptyJoinStream {
    fn next(&mut self) -> Result<Option<DataBlock>> {
        Ok(None)
    }
}

pub struct OneBlockJoinStream(pub Option<DataBlock>);

impl JoinStream for OneBlockJoinStream {
    fn next(&mut self) -> Result<Option<DataBlock>> {
        Ok(self.0.take())
    }
}
