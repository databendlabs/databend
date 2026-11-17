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

use std::sync::Arc;

use databend_meta_client::ClientHandle;
use databend_meta_client::types::SeqV;
use databend_meta_runtime::DatabendRuntime;
use parking_lot::Mutex;

use crate::Cache;

/// A [`Cache`] of the keys under one prefix, shared by its readers.
///
/// Its reads are those of [`Cache`]: they answer only if the copy can answer at once, and the
/// caller reads meta-service for every `None`.
pub struct WatchedPrefix {
    /// Held only to poll the copy once, never across an await, so readers do not queue behind
    /// one another.
    cache: Mutex<Cache>,
}

impl WatchedPrefix {
    /// Start watching the keys `{prefix}/...`. A trailing `/` of `prefix` is ignored.
    pub async fn new(
        client: Arc<ClientHandle<DatabendRuntime>>,
        prefix: &str,
        name: impl ToString,
    ) -> Self {
        let cache = Cache::new(client, prefix, name).await;
        WatchedPrefix {
            cache: Mutex::new(cache),
        }
    }

    /// See [`Cache::try_get_ready`].
    pub fn try_get_ready(&self, key: &str) -> Option<SeqV> {
        self.cache.lock().try_get_ready(key)
    }

    /// See [`Cache::try_list_dir_ready`].
    pub fn try_list_dir_ready(&self, dir: &str) -> Option<Vec<(String, SeqV)>> {
        self.cache.lock().try_list_dir_ready(dir)
    }
}
