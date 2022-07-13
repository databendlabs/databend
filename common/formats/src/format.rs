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

use std::any::Any;

use common_datablocks::DataBlock;
use common_datavalues::TypeDeserializerImpl;
use common_exception::ErrorCode;
use common_exception::Result;
use common_io::prelude::MemoryReader;
use common_io::prelude::NestedCheckpointReader;

pub trait InputState: Send {
    fn as_any(&mut self) -> &mut dyn Any;
}

pub trait InputFormat: Send + Sync {
    fn support_parallel(&self) -> bool {
        false
    }

    fn create_state(&self) -> Box<dyn InputState>;

    fn set_state(
        &self,
        _state: &mut Box<dyn InputState>,
        _file_name: String,
        _start_row_index: usize,
    ) -> Result<()> {
        Err(ErrorCode::UnImplement("Unimplement error"))
    }

    fn deserialize_data(&self, state: &mut Box<dyn InputState>) -> Result<Vec<DataBlock>>;

    fn read_buf(&self, buf: &[u8], state: &mut Box<dyn InputState>) -> Result<usize>;

    fn skip_header(&self, buf: &[u8], state: &mut Box<dyn InputState>) -> Result<usize>;

    fn read_row(
        &self,
        _checkpoint_reader: &mut NestedCheckpointReader<MemoryReader>,
        _deserializers: &mut Vec<TypeDeserializerImpl>,
        _row_index: usize,
    ) -> Result<()> {
        Err(ErrorCode::UnImplement("Unimplement error"))
    }

    fn read_row_num(&self, _state: &mut Box<dyn InputState>) -> Result<usize> {
        Err(ErrorCode::UnImplement("Unimplement error"))
    }
}
