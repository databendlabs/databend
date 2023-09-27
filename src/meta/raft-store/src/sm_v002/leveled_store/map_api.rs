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
use std::fmt;
use std::future::Future;
use std::ops::RangeBounds;

use common_meta_types::KVMeta;
use futures_util::stream::BoxStream;

use crate::sm_v002::marked::Marked;

/// MapKey defines the behavior of a key in a map.
///
/// It is `Clone` to let MapApi clone a range of key.
/// It is `Unpin` to let MapApi extract a key from pinned data, such as a stream.
/// And it only accepts `'static` value for simplicity.
pub(in crate::sm_v002) trait MapKey:
    Clone + Ord + fmt::Debug + Send + Sync + Unpin + 'static
{
    type V: MapValue;
}

/// MapValue defines the behavior of a value in a map.
///
/// It is `Clone` to let MapApi return an owned value.
/// It is `Unpin` to let MapApi extract a value from pinned data, such as a stream.
/// And it only accepts `'static` value for simplicity.
pub(in crate::sm_v002) trait MapValue:
    Clone + Send + Sync + Unpin + 'static
{
}

// Auto implement MapValue for all types that satisfy the constraints.
impl<V> MapValue for V where V: Clone + Send + Sync + Unpin + 'static {}

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
pub(in crate::sm_v002) trait MapApiRO<'d, K>: Send + Sync
where K: MapKey
{
    type GetFut<'f, Q>: Future<Output = Marked<K::V>>
    where
        Self: 'f,
        'd: 'f,
        K: Borrow<Q>,
        Q: Ord + Send + Sync + ?Sized,
        Q: 'f;

    /// Get an entry by key.
    fn get<'f, Q>(self, key: &'f Q) -> Self::GetFut<'f, Q>
    where
        K: Borrow<Q>,
        Q: Ord + Send + Sync + ?Sized,
        Q: 'f;

    type RangeFut<'f, Q, R>: Future<Output = BoxStream<'f, (K, Marked<K::V>)>>
    where
        Self: 'f,
        'd: 'f,
        K: Borrow<Q>,
        R: RangeBounds<Q> + Send + Sync + Clone,
        R: 'f,
        Q: Ord + Send + Sync + ?Sized,
        Q: 'f;

    /// Iterate over a range of entries by keys.
    ///
    /// The returned iterator contains tombstone entries: [`Marked::TombStone`].
    fn range<'f, Q, R>(self, range: R) -> Self::RangeFut<'f, Q, R>
    where
        K: Borrow<Q>,
        Q: Ord + Send + Sync + ?Sized,
        R: RangeBounds<Q> + Send + Sync + Clone,
        R: 'f;
}

/// Provide a read-write key-value map API set, used to access state machine data.
pub(in crate::sm_v002) trait MapApi<'d, K>: MapApiRO<'d, K>
where K: MapKey
{
    type RO<'o>: MapApiRO<'o, K>
    where Self: 'o;

    fn to_ro<'o>(&'o self) -> Self::RO<'o>;

    type SetFut<'f>: Future<Output = (Marked<K::V>, Marked<K::V>)>
    where Self: 'f;

    /// Set an entry and returns the old value and the new value.
    fn set<'f>(self, key: K, value: Option<(K::V, Option<KVMeta>)>) -> Self::SetFut<'f>;
}

pub(in crate::sm_v002) struct MapApiExt;

impl MapApiExt {
    /// Update only the meta associated to an entry and keeps the value unchanged.
    /// If the entry does not exist, nothing is done.
    pub(in crate::sm_v002) async fn update_meta<'d, K, T>(
        s: &'d mut T,
        key: K,
        meta: Option<KVMeta>,
    ) -> (Marked<K::V>, Marked<K::V>)
    where
        K: MapKey,
        &'d mut T: MapApi<'d, K>,
    {
        //
        let got = s.to_ro().get(&key).await;
        if got.is_tomb_stone() {
            return (got.clone(), got.clone());
        }

        // Safe unwrap(), got is Normal
        let (v, _) = got.unpack_ref().unwrap();

        s.set(key, Some((v.clone(), meta))).await
    }

    /// Update only the value and keeps the meta unchanged.
    /// If the entry does not exist, create one.
    pub(in crate::sm_v002) async fn upsert_value<'d, K, T>(
        s: &'d mut T,
        key: K,
        value: K::V,
    ) -> (Marked<K::V>, Marked<K::V>)
    where
        K: MapKey,
        &'d mut T: MapApi<'d, K>,
    {
        let got = s.to_ro().get(&key).await;

        let meta = if let Some((_, meta)) = got.unpack_ref() {
            meta
        } else {
            None
        };

        s.set(key, Some((value, meta.cloned()))).await
    }
}

impl<'ro_me, 'ro_d, K, T> MapApiRO<'ro_d, K> for &'ro_me mut T
where
    K: MapKey,
    &'ro_me T: MapApiRO<'ro_d, K>,
    K: Ord + Send + Sync + 'static,
    T: Send + Sync,
{
    type GetFut<'f, Q> = <&'ro_me T as MapApiRO<'ro_d, K>>::GetFut<'f, Q>
        where
            Self: 'f,
            'ro_me: 'f,
            'ro_d: 'f,
            K: Borrow<Q>,
            Q: Ord + Send + Sync + ?Sized,
            Q: 'f;

    fn get<'f, Q>(self, key: &'f Q) -> Self::GetFut<'f, Q>
    where
        'ro_me: 'f,
        'ro_d: 'f,
        K: Borrow<Q>,
        Q: Ord + Send + Sync + ?Sized,
    {
        (&*self).get(key)
    }

    type RangeFut<'f, Q, R> = <&'ro_me T as MapApiRO<'ro_d, K>>::RangeFut<'f, Q, R>
    where
        Self: 'f,
        'ro_me: 'f,
        'ro_d: 'f,
        K: Borrow<Q>,
        R: RangeBounds<Q> + Send + Sync + Clone,
    R: 'f,
        Q: Ord + Send + Sync + ?Sized,
        Q: 'f;

    fn range<'f, Q, R>(self, range: R) -> Self::RangeFut<'f, Q, R>
    where
        'ro_me: 'f,
        'ro_d: 'f,
        K: Borrow<Q>,
        Q: Ord + Send + Sync + ?Sized,
        R: RangeBounds<Q> + Clone + Send + Sync,
        R: 'f,
    {
        (&*self).range(range)
    }
}
