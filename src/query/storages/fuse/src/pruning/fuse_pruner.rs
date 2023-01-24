//  Copyright 2023 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::sync::Arc;

use common_base::base::tokio::sync::Semaphore;
use common_base::runtime::Runtime;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use opendal::Operator;
use storages_common_pruner::Limiter;
use storages_common_pruner::PagePruner;
use storages_common_pruner::RangePruner;

use crate::pruning::FuseBloomPruner;

#[derive(Clone)]
pub struct PruningContext {
    pub ctx: Arc<dyn TableContext>,
    pub dal: Operator,
    pub pruning_runtime: Arc<Runtime>,
    pub pruning_semaphore: Arc<Semaphore>,

    pub limit_pruner: Arc<dyn Limiter + Send + Sync>,
    pub range_pruner: Arc<dyn RangePruner + Send + Sync>,
    pub filter_pruner: Option<Arc<dyn FuseBloomPruner + Send + Sync>>,
    pub page_pruner: Arc<dyn PagePruner + Send + Sync>,
}

pub struct FusePruner {}

impl FusePruner {
    pub fn create_ctx() -> Result<PruningContext> {
        todo!()
    }
}
