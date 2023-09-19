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
use std::ops::RangeBounds;

use common_meta_types::KVMeta;
use futures_util::stream::BoxStream;

use crate::sm_v002::marked::Marked;

/// Provide a readonly key-value map API set, used to access state machine data.
///
/// `MapApiRO` and `MapApi` both have two lifetime parameters, `'me` and `'d`,
/// to describe the lifetime of the MapApi object and the lifetime of the data.
///
/// When an implementation owns the data it operates on, `'me` must outlive `'d`.
/// Otherwise, i.e., the implementation just keeps a reference to the data,
/// `'me` could be shorter than `'d`.
///
/// There is no lifetime constraint on the trait,
/// and it's the implementation's duty to specify a valid lifetime constraint.
#[async_trait::async_trait]
pub(in crate::sm_v002) trait MapApiRO<'me, 'd, K>: Send + Sync
where K: Ord + Send + Sync + 'static
{
    type V: Clone + Send + Sync + 'static;

    /// Get an entry by key.
    async fn get<Q>(self, key: &'d Q) -> Marked<Self::V>
    where
        K: Borrow<Q>,
        Q: Ord + Send + Sync + ?Sized;

    /// Iterate over a range of entries by keys.
    ///
    /// The returned iterator contains tombstone entries: [`Marked::TombStone`].
    async fn range<T: ?Sized, R>(self, range: R) -> BoxStream<'d, (K, Marked<Self::V>)>
    where
        K: Clone + Borrow<T>,
        Self::V: Unpin,
        T: Ord,
        R: RangeBounds<T> + Send + Sync + Clone;
}

/// Provide a read-write key-value map API set, used to access state machine data.
#[async_trait::async_trait]
pub(in crate::sm_v002) trait MapApi<'me, 'd, K>:
    MapApiRO<'me, 'd, K> + Send + Sync
where K: Ord + Send + Sync + 'static
{
    /// Set an entry and returns the old value and the new value.
    async fn set(
        mut self,
        key: K,
        value: Option<(<Self as MapApiRO<'me, 'd, K>>::V, Option<KVMeta>)>,
    ) -> (
        Marked<<Self as MapApiRO<'me, 'd, K>>::V>,
        Marked<<Self as MapApiRO<'me, 'd, K>>::V>,
    );

    /// Update only the meta associated to an entry and keeps the value unchanged.
    /// If the entry does not exist, nothing is done.
    async fn update_meta(
        self,
        key: K,
        meta: Option<KVMeta>,
    ) -> (
        Marked<<Self as MapApiRO<'me, 'd, K>>::V>,
        Marked<<Self as MapApiRO<'me, 'd, K>>::V>,
    )
    where
        Self: Sized,
    {
        //
        let got = self.get(&key).await;
        if got.is_tomb_stone() {
            return (got.clone(), got.clone());
        }

        // Safe unwrap(), got is Normal
        let (v, _) = got.unpack_ref().unwrap();

        self.set(key, Some((v.clone(), meta))).await
    }

    /// Update only the value and keeps the meta unchanged.
    /// If the entry does not exist, create one.
    async fn upsert_value(
        &'me mut self,
        key: K,
        value: <Self as MapApiRO<'me, 'd, K>>::V,
        // TODO: use Self::V
    ) -> (
        Marked<<Self as MapApiRO<'me, 'd, K>>::V>,
        Marked<<Self as MapApiRO<'me, 'd, K>>::V>,
    ) {
        let got = self.get(&key).await;

        let meta = if let Some((_, meta)) = got.unpack_ref() {
            meta
        } else {
            None
        };

        self.set(key, Some((value, meta.cloned()))).await
    }
}

#[async_trait::async_trait]
impl<'me, 'd, K, T> MapApiRO<'me, 'd, K> for &'me mut T
where
    &'me T: MapApiRO<'me, 'd, K>,
    K: Ord + Send + Sync + 'static,
    T: Send + Sync,
{
    type V = <&'me T as MapApiRO<'me, 'd, K>>::V;

    async fn get<Q>(self, key: &'d Q) -> Marked<Self::V>
    where
        K: Borrow<Q>,
        Q: Ord + Send + Sync + ?Sized,
    {
        (&*self).get(key).await
    }

    async fn range<Q: ?Sized, R>(self, range: R) -> BoxStream<'d, (K, Marked<Self::V>)>
    where
        K: Clone + Borrow<Q>,
        Self::V: Unpin,
        Q: Ord,
        R: RangeBounds<Q> + Send + Sync + Clone,
    {
        (&*self).range(range).await
    }
}
