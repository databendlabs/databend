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
use std::io;
use std::ops::RangeBounds;
use std::sync::Arc;

use crate::sm_v002::leveled_store::level::Level;
use crate::sm_v002::leveled_store::map_api::AsMap;
use crate::sm_v002::leveled_store::map_api::KVResultStream;
use crate::sm_v002::leveled_store::map_api::MapApiRO;
use crate::sm_v002::leveled_store::map_api::MapKV;
use crate::sm_v002::leveled_store::map_api::MapKey;
use crate::sm_v002::leveled_store::map_api::MarkedOf;
use crate::sm_v002::marked::Marked;
use crate::state_machine::ExpireKey;

impl Level {
    /// Build a static stream that yields key values for primary index
    #[futures_async_stream::try_stream(boxed, ok = MapKV<String>, error = io::Error)]
    async fn str_range<Q, R>(self: Arc<Level>, range: R)
    where
        String: Borrow<Q>,
        Q: Ord + Send + Sync + ?Sized,
        R: RangeBounds<Q> + Clone + Send + Sync + 'static,
    {
        let it = self.as_ref().kv.range(range);

        for (k, v) in it {
            yield (k.clone(), v.clone());
        }
    }

    /// Build a static stream that yields expire key and key for the secondary expiration index
    #[futures_async_stream::try_stream(boxed, ok = MapKV<ExpireKey>, error = io::Error)]
    async fn expire_range<Q, R>(self: Arc<Level>, range: R)
    where
        ExpireKey: Borrow<Q>,
        Q: Ord + Send + Sync + ?Sized,
        R: RangeBounds<Q> + Clone + Send + Sync + 'static,
    {
        let it = self.as_ref().expire.range(range);

        for (k, v) in it {
            yield (*k, v.clone());
        }
    }
}

#[async_trait::async_trait]
impl MapApiRO<String> for Arc<Level> {
    async fn get<Q>(&self, key: &Q) -> Result<Marked<<String as MapKey>::V>, io::Error>
    where
        String: Borrow<Q>,
        Q: Ord + Send + Sync + ?Sized,
    {
        // get() is just delegated
        self.as_ref().str_map().get(key).await
    }

    async fn range<R>(&self, range: R) -> Result<KVResultStream<String>, io::Error>
    where R: RangeBounds<String> + Clone + Send + Sync + 'static {
        let strm = self.clone().str_range(range);
        Ok(strm)
    }
}

#[async_trait::async_trait]
impl MapApiRO<ExpireKey> for Arc<Level> {
    async fn get<Q>(&self, key: &Q) -> Result<MarkedOf<ExpireKey>, io::Error>
    where
        ExpireKey: Borrow<Q>,
        Q: Ord + Send + Sync + ?Sized,
    {
        // get() is just delegated
        self.as_ref().expire_map().get(key).await
    }

    async fn range<R>(&self, range: R) -> Result<KVResultStream<ExpireKey>, io::Error>
    where R: RangeBounds<ExpireKey> + Clone + Send + Sync + 'static {
        let strm = self.clone().expire_range(range);
        Ok(strm)
    }
}
