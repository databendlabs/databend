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

use crate::ObjectCache;

pub struct ObjectReader<T> {
    cache: Arc<dyn ObjectCache<T>>,
}

impl<T> ObjectReader<T> {
    pub fn create(cache: Arc<dyn ObjectCache<T>>) -> ObjectReader<T> {
        Self { cache }
    }

    pub async fn read(&self, object: &Object, start: u64, end: u64) -> Result<T> {
        self.cache.read_object(object, start, end).await
    }
}
