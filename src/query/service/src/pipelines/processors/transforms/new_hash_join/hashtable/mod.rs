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

mod fixed_keys;

mod serialize_keys;
mod single_binary_key;
pub mod basic;

use databend_common_column::bitmap::Bitmap;
use databend_common_expression::BlockEntry;
use databend_common_expression::DataBlock;

pub struct ProbeData {
    keys: DataBlock,
    valids: Option<Bitmap>,
}

impl ProbeData {
    pub fn new(keys: DataBlock, valids: Option<Bitmap>) -> Self {
        Self { keys, valids }
    }

    pub fn num_rows(&self) -> usize {
        self.keys.num_rows()
    }

    pub fn columns(&self) -> &[BlockEntry] {
        self.keys.columns()
    }

    pub fn non_null_rows(&self) -> usize {
        match &self.valids {
            None => self.keys.num_rows(),
            Some(valids) => valids.len() - valids.null_count(),
        }
    }

    pub fn into_raw(self) -> (DataBlock, Option<Bitmap>) {
        (self.keys, self.valids)
    }
}
