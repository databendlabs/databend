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

use std::io;
use std::ops::Deref;
use std::ops::RangeBounds;
use std::sync::Arc;

use crate::impls::level::Level;
use crate::KVResultStream;
use crate::MapApiRO;
use crate::MarkedOf;

/// A single **immutable** level data.
///
/// Only used for testing.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) struct Immutable<M = ()> {
    /// An in-process unique to identify this immutable level.
    ///
    /// It is used to assert an immutable level is not replaced after compaction.
    level: Arc<Level<M>>,
}

impl<M> Immutable<M> {
    #[allow(dead_code)]
    fn new(level: Arc<Level<M>>) -> Self {
        Self { level }
    }

    #[allow(dead_code)]
    pub(crate) fn new_from_level(level: Level<M>) -> Self {
        Self::new(Arc::new(level))
    }
}

impl<M> AsRef<Level<M>> for Immutable<M> {
    fn as_ref(&self) -> &Level<M> {
        self.level.as_ref()
    }
}

impl<M> Deref for Immutable<M> {
    type Target = Level<M>;

    fn deref(&self) -> &Self::Target {
        self.level.as_ref()
    }
}

#[async_trait::async_trait]
impl<M> MapApiRO<String, M> for Immutable<M>
where M: Clone + Unpin + Send + Sync + 'static
{
    async fn get(&self, key: &String) -> Result<MarkedOf<String, M>, io::Error> {
        self.level.get(key).await
    }

    async fn range<R>(&self, range: R) -> Result<KVResultStream<String, M>, io::Error>
    where R: RangeBounds<String> + Clone + Send + Sync + 'static {
        let strm = self.level.range(range).await?;
        Ok(strm)
    }
}
