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

use std::sync::Arc;

use common_exception::Result;
use opendal::Object;

use crate::CacheSettings;
use crate::ObjectCacheProvider;

// TODO(bohu): local disk file based LRU cache
pub struct FileCache {}

/// By pass cache, ignore the cache, default.
impl FileCache {
    pub fn create(_settings: &CacheSettings) -> FileCache {
        Self {}
    }
}

#[async_trait::async_trait]
impl ObjectCacheProvider<Vec<u8>> for FileCache {
    async fn read_object(&self, _object: &Object, _start: u64, _end: u64) -> Result<Arc<Vec<u8>>> {
        todo!()
    }

    async fn write_object(&self, _object: &Object, _v: Arc<Vec<u8>>) -> Result<()> {
        todo!()
    }

    async fn remove_object(&self, object: &Object) -> Result<()> {
        object.delete().await?;
        Ok(())
    }
}
