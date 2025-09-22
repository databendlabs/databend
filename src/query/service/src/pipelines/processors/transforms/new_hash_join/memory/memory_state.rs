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

use std::collections::VecDeque;
use std::sync::Mutex;

use databend_common_expression::DataBlock;

use crate::pipelines::processors::transforms::new_hash_join::common::CStyleCell;
use crate::pipelines::processors::transforms::HashJoinHashTable;

pub struct HashJoinMemoryState {
    pub mutex: Mutex<()>,
    pub build_rows: CStyleCell<usize>,
    pub chunks: CStyleCell<Vec<DataBlock>>,
    pub build_queue: CStyleCell<VecDeque<usize>>,

    pub hash_table: CStyleCell<HashJoinHashTable>,
    pub arenas: CStyleCell<Vec<Vec<u8>>>,
}
