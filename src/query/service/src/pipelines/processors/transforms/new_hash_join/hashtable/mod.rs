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

pub mod basic;
mod serialize_keys;
mod single_binary_key;

use databend_common_column::bitmap::Bitmap;
use databend_common_expression::BlockEntry;
use databend_common_expression::DataBlock;

pub struct ProbeHashStatistics {
    probed_rows: usize,
    matched_rows: usize,

    selection: Vec<u32>,
    unmatched_selection: Vec<u32>,
}

impl ProbeHashStatistics {
    pub fn new(max_rows: usize) -> Self {
        ProbeHashStatistics {
            probed_rows: 0,
            matched_rows: 0,
            selection: Vec::with_capacity(max_rows),
            unmatched_selection: Vec::with_capacity(max_rows),
        }
    }

    pub fn clear(&mut self) {
        self.selection.clear();
        self.unmatched_selection.clear();
    }
}

pub struct ProbeData<'a> {
    keys: DataBlock,
    valids: Option<Bitmap>,
    probe_hash_statistics: &'a mut ProbeHashStatistics,
}

impl<'a> ProbeData<'a> {
    pub fn new(
        keys: DataBlock,
        valids: Option<Bitmap>,
        probe_hash_statistics: &'a mut ProbeHashStatistics,
    ) -> Self {
        ProbeData {
            keys,
            valids,
            probe_hash_statistics,
        }
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

    pub fn into_raw(self) -> (DataBlock, Option<Bitmap>, &'a mut ProbeHashStatistics) {
        (self.keys, self.valids, self.probe_hash_statistics)
    }
}
