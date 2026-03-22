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
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::LazyLock;

use databend_common_expression::DataBlock;
use parking_lot::RwLock;
use tokio::sync::watch;
/// Shared store to support memory tables.
///
/// Indexed by table id etc.
pub type InMemoryData<K> = HashMap<K, Arc<RwLock<Vec<DataBlock>>>>;

#[derive(Debug, Default, Clone, Copy)]
pub struct RecursiveReaderState {
    pub generation: Option<usize>,
    pub block_index: usize,
}

#[derive(Debug, Default)]
pub struct InMemoryRecursiveState {
    pub generations: BTreeMap<usize, Vec<DataBlock>>,
    pub readers: HashMap<u64, RecursiveReaderState>,
    pub next_reader_id: u64,
    pub active_generation: Option<usize>,
    pub cached_output: Vec<DataBlock>,
    pub sealed: bool,
    pub running: bool,
}

#[derive(Debug)]
pub struct InMemoryRecursiveDataEntry {
    pub state: RwLock<InMemoryRecursiveState>,
    pub state_change_tx: watch::Sender<u64>,
}

impl Default for InMemoryRecursiveDataEntry {
    fn default() -> Self {
        let (state_change_tx, _) = watch::channel(0);
        Self {
            state: RwLock::new(InMemoryRecursiveState::default()),
            state_change_tx,
        }
    }
}

pub type InMemoryRecursiveData<K> = HashMap<K, Arc<InMemoryRecursiveDataEntry>>;

pub static IN_MEMORY_DATA: LazyLock<Arc<RwLock<InMemoryData<InMemoryDataKey>>>> =
    LazyLock::new(|| Arc::new(Default::default()));

pub static IN_MEMORY_R_CTE_DATA: LazyLock<Arc<RwLock<InMemoryRecursiveData<InMemoryDataKey>>>> =
    LazyLock::new(|| Arc::new(Default::default()));

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct InMemoryDataKey {
    pub temp_prefix: Option<String>,
    pub table_id: u64,
}
