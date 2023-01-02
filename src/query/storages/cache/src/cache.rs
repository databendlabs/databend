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

#[async_trait::async_trait]
pub trait ObjectCacheProvider<T> {
    async fn read_object(&self, object: &Object, start: u64, end: u64) -> Result<Arc<T>>;
    async fn write_object(&self, object: &Object, v: Arc<T>) -> Result<()>;
    async fn remove_object(&self, object: &Object) -> Result<()>;
}
