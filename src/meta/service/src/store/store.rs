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

use std::ops::Deref;
use std::sync::Arc;

use databend_common_meta_raft_store::config::RaftConfig;
use databend_common_meta_types::MetaStartupError;

use crate::store::StoreInner;

/// A store that implements `RaftStorage` trait and provides full functions.
///
/// It is designed to be cloneable in order to be shared by MetaNode and Raft.
#[derive(Clone)]
pub struct RaftStore {
    pub(crate) inner: Arc<StoreInner>,
}

impl RaftStore {
    pub fn new(sto: StoreInner) -> Self {
        Self {
            inner: Arc::new(sto),
        }
    }

    #[fastrace::trace]
    pub async fn open_create(
        config: &RaftConfig,
        open: Option<()>,
        create: Option<()>,
    ) -> Result<Self, MetaStartupError> {
        let sto = StoreInner::open_create(config, open, create).await?;
        Ok(Self::new(sto))
    }

    pub fn inner(&self) -> Arc<StoreInner> {
        self.inner.clone()
    }
}

impl Deref for RaftStore {
    type Target = StoreInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
