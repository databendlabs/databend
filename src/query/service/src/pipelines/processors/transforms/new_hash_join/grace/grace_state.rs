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

use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex;

use parquet::file::metadata::RowGroupMetaData;

use crate::pipelines::processors::transforms::HashJoinFactory;
use crate::pipelines::processors::transforms::new_hash_join::common::CStyleCell;
use crate::sessions::QueryContext;

#[derive(Debug)]
pub struct SpillMetadata {
    pub path: String,
    pub row_groups: Vec<RowGroupMetaData>,
}

pub struct GraceHashJoinState {
    pub mutex: Mutex<()>,
    pub ctx: Arc<QueryContext>,
    pub finished: CStyleCell<bool>,
    pub restore_partition: CStyleCell<Option<usize>>,
    pub restore_build_queue: CStyleCell<VecDeque<SpillMetadata>>,
    pub restore_probe_queue: CStyleCell<VecDeque<SpillMetadata>>,
    pub build_row_groups: CStyleCell<BTreeMap<usize, Vec<SpillMetadata>>>,
    pub probe_row_groups: CStyleCell<BTreeMap<usize, Vec<SpillMetadata>>>,

    level: usize,
    factory: Arc<HashJoinFactory>,
}

impl GraceHashJoinState {
    pub fn create(
        ctx: Arc<QueryContext>,
        level: usize,
        factory: Arc<HashJoinFactory>,
    ) -> Arc<GraceHashJoinState> {
        Arc::from(GraceHashJoinState {
            ctx,
            level,
            factory,
            mutex: Mutex::new(()),
            finished: CStyleCell::new(false),
            build_row_groups: CStyleCell::new(BTreeMap::new()),
            probe_row_groups: CStyleCell::new(BTreeMap::new()),
            restore_build_queue: CStyleCell::new(VecDeque::new()),
            restore_probe_queue: CStyleCell::new(VecDeque::new()),
            restore_partition: CStyleCell::new(None),
        })
    }
}

impl Drop for GraceHashJoinState {
    fn drop(&mut self) {
        self.factory.remove_grace_state(self.level);
    }
}
