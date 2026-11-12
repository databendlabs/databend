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

//! Read each selected block once before index fan-out.

use std::collections::VecDeque;
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_pipeline::sources::AsyncSource;
use databend_storages_common_io::ReadSettings;
use parking_lot::Mutex;

use crate::FuseStorageFormat;
use crate::operations::table_index::meta::RefreshBlock;
use crate::operations::table_index::meta::RefreshInput;

pub struct ReadRefreshBlock {
    queue: Arc<Mutex<VecDeque<Arc<RefreshBlock>>>>,
    settings: ReadSettings,
    format: FuseStorageFormat,
}

impl ReadRefreshBlock {
    pub fn new(
        queue: Arc<Mutex<VecDeque<Arc<RefreshBlock>>>>,
        settings: ReadSettings,
        format: FuseStorageFormat,
    ) -> Self {
        Self {
            queue,
            settings,
            format,
        }
    }
}

#[async_trait::async_trait]
impl AsyncSource for ReadRefreshBlock {
    const NAME: &'static str = "ReadRefreshBlock";
    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        let plan = self.queue.lock().pop_front();
        let Some(plan) = plan else {
            return Ok(None);
        };
        let block = plan
            .reader
            .read_by_meta(&self.settings, &plan.meta, &self.format)
            .await?;
        Ok(Some(block.add_meta(Some(Box::new(RefreshInput(plan))))?))
    }
}
