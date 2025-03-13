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

//! Defines MapApiRO.

use std::io;
use std::ops::RangeBounds;

use crate::map_key::MapKey;
use crate::KVResultStream;
use crate::MarkedOf;

/// Provide a readonly key-value map API set.
#[async_trait::async_trait]
pub trait MapApiRO<K, M>: Send + Sync
where K: MapKey<M>
{
    // The following does not work, because MapKeyEncode is defined in the application crate,
    // But using `Q` in the defining crate requires `MapKeyEncode`.
    // Because the application crate can not add more constraints to `Q`.
    // async fn get<Q>(&self, key: &Q) -> Result<MarkedOf<K>, io::Error>
    // where
    //     K: Borrow<Q>,
    //     Q: Ord + Send + Sync + ?Sized,
    //     Q: MapKeyEncode;

    /// Get an entry by key.
    async fn get(&self, key: &K) -> Result<MarkedOf<K, M>, io::Error>;

    /// Iterate over a range of entries by keys.
    ///
    /// The returned iterator contains tombstone entries: [Marked::TombStone](crate::marked::Marked::TombStone).
    async fn range<R>(&self, range: R) -> Result<KVResultStream<K, M>, io::Error>
    where R: RangeBounds<K> + Send + Sync + Clone + 'static;
}

#[async_trait::async_trait]
impl<K, M, T> MapApiRO<K, M> for &T
where
    T: MapApiRO<K, M>,
    K: MapKey<M>,
{
    async fn get(&self, key: &K) -> Result<MarkedOf<K, M>, io::Error> {
        (**self).get(key).await
    }

    async fn range<R>(&self, range: R) -> Result<KVResultStream<K, M>, io::Error>
    where R: RangeBounds<K> + Send + Sync + Clone + 'static {
        (**self).range(range).await
    }
}

mod impls {
    use std::io;
    use std::ops::RangeBounds;

    use futures_util::StreamExt;

    use crate::map_api_ro::MapApiRO;
    use crate::map_key::MapKey;
    use crate::marked::Marked;
    use crate::KVResultStream;

    /// Dummy implementation of [`MapApiRO`] for `()`.
    /// So that () can be used as a placeholder where a [`MapApiRO`] is expected.
    #[async_trait::async_trait]
    impl<K, M> MapApiRO<K, M> for ()
    where
        K: MapKey<M>,
        M: Send + 'static,
    {
        async fn get(&self, _key: &K) -> Result<Marked<M, K::V>, io::Error> {
            Ok(Marked::empty())
        }

        async fn range<R>(&self, _range: R) -> Result<KVResultStream<K, M>, io::Error>
        where R: RangeBounds<K> + Send + Sync + Clone + 'static {
            Ok(futures::stream::iter([]).boxed())
        }
    }
}
