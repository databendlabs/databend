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

use databend_common_base::base::Progress;
use databend_common_base::base::ProgressValues;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;

pub trait JoinStream: Send + Sync + 'static {
    fn next(&mut self) -> Result<Option<DataBlock>>;
}

pub trait Join: Send + Sync + 'static {
    fn add_block(&mut self, data: Option<DataBlock>) -> Result<()>;

    fn final_build(&mut self) -> Result<Option<ProgressValues>>;

    fn probe_block(&mut self, data: DataBlock) -> Result<Box<dyn JoinStream>>;

    fn final_probe(&mut self) -> Result<Box<dyn JoinStream>>;
}

pub struct EmptyJoinStream;

impl JoinStream for EmptyJoinStream {
    fn next(&mut self) -> Result<Option<DataBlock>> {
        Ok(None)
    }
}
