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

use common_exception::Result;
use opendal::Object;

use crate::ObjectCache;

pub struct ByPassCache {}

impl ByPassCache {
    pub fn create() -> ByPassCache {
        Self {}
    }
}

#[async_trait::async_trait]
impl ObjectCache<Vec<u8>> for ByPassCache {
    async fn read_object(&self, object: &Object, start: u64, end: u64) -> Result<Vec<u8>> {
        let data = object.range_read(start..end).await?;
        Ok(data)
    }

    async fn write_object(&self, object: &Object, bs: Vec<u8>) -> Result<()> {
        object.write(bs).await?;
        Ok(())
    }

    async fn remove_object(&self, object: &Object) -> Result<()> {
        object.delete().await?;
        Ok(())
    }
}
