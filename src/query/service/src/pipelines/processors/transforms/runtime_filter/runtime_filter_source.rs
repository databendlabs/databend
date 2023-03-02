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

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;

use common_base::base::tokio::sync::Notify;
use common_exception::Result;
use common_expression::DataBlock;
use common_expression::RemoteExpr;
use common_sql::plans::RuntimeFilterId;
use parking_lot::Mutex;
use storages_common_index::filters::Xor8Filter;

use crate::pipelines::processors::transforms::runtime_filter::RuntimeFilterConnector;

pub struct RuntimeFilterState {
    pub(crate) channel_filters: Arc<Mutex<HashMap<RuntimeFilterId, Xor8Filter>>>,
    pub(crate) left_runtime_filters: BTreeMap<RuntimeFilterId, RemoteExpr>,
    pub(crate) right_runtime_filters: BTreeMap<RuntimeFilterId, RemoteExpr>,
    pub(crate) sinker_count: Mutex<usize>,
    pub(crate) finished_notify: Arc<Notify>,
    pub(crate) finished: Mutex<bool>,
}

impl RuntimeFilterState {
    pub fn new(
        left_runtime_filters: BTreeMap<RuntimeFilterId, RemoteExpr>,
        right_runtime_filters: BTreeMap<RuntimeFilterId, RemoteExpr>,
    ) -> Self {
        RuntimeFilterState {
            channel_filters: Arc::new(Mutex::new(Default::default())),
            left_runtime_filters,
            right_runtime_filters,
            sinker_count: Mutex::new(0),
            finished_notify: Arc::new(Default::default()),
            finished: Default::default(),
        }
    }
}

#[async_trait::async_trait]
impl RuntimeFilterConnector for RuntimeFilterState {
    fn attach(&self) {
        let mut sinker_count = self.sinker_count.lock();
        *sinker_count += 1;
    }

    fn detach(&self) -> Result<()> {
        let mut sinker_count = self.sinker_count.lock();
        *sinker_count -= 1;
        if *sinker_count == 0 {
            let mut finished = self.finished.lock();
            *finished = true;
            self.finished_notify.notify_waiters();
        }
        Ok(())
    }

    fn is_finished(&self) -> Result<bool> {
        Ok(*self.finished.lock())
    }

    async fn wait_finish(&self) -> Result<()> {
        if !self.is_finished()? {
            self.finished_notify.notified().await;
        }
        Ok(())
    }

    fn consume(&self, data: &DataBlock) -> Result<Vec<DataBlock>> {
        todo!()
    }

    fn collect(&self, data: &DataBlock) -> Result<()> {
        todo!()
    }
}
