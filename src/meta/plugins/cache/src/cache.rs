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
use std::ops::DerefMut;
use std::sync::Arc;

use databend_meta_client::ClientHandle;
use databend_meta_runtime::DatabendRuntime;

use crate::meta_cache_types::MetaCacheTypes;
use crate::meta_client_source;

pub struct Cache {
    pub(crate) inner: sub_cache::Cache<MetaCacheTypes>,
}

impl Deref for Cache {
    type Target = sub_cache::Cache<MetaCacheTypes>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for Cache {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl Cache {
    /// Create a new cache with the given prefix and meta client.
    ///
    /// The cache will start watching for changes immediately.
    ///
    /// # Parameters
    ///
    /// * `meta_client` - The meta client to interact with the remote data store.
    /// * `prefix` - The prefix for the cache, used to identify the cache instance.
    /// * `name` - The name of the cache, used for debugging and logging purposes.
    pub async fn new(
        meta_client: Arc<ClientHandle<DatabendRuntime>>,
        prefix: impl ToString,
        name: impl ToString,
    ) -> Self {
        let name = name.to_string();
        let inner = sub_cache::Cache::new(
            meta_client_source::MetaClientSource {
                client: meta_client,
                name: name.clone(),
            },
            prefix,
            name,
        )
        .await;

        Cache { inner }
    }
}
