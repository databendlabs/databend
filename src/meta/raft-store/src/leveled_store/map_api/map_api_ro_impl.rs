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

use std::borrow::Borrow;
use std::io::Error;
use std::ops::RangeBounds;

use futures_util::StreamExt;

use crate::leveled_store::map_api::KVResultStream;
use crate::leveled_store::map_api::MapApiRO;
use crate::leveled_store::map_api::MapKey;
use crate::marked::Marked;

/// Dummy implementation of [`MapApiRO`] for `()`.
/// So that () can be used as a place holder where a [`MapApiRO`] is expected.
#[async_trait::async_trait]
impl<K> MapApiRO<K> for ()
where K: MapKey
{
    async fn get<Q>(&self, _key: &Q) -> Result<Marked<K::V>, Error>
    where
        K: Borrow<Q>,
        Q: Ord + Send + Sync + ?Sized,
    {
        Ok(Marked::empty())
    }

    async fn range<R>(&self, _range: R) -> Result<KVResultStream<K>, Error>
    where R: RangeBounds<K> + Send + Sync + Clone + 'static {
        Ok(futures::stream::iter([]).boxed())
    }
}
