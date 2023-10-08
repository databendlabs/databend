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
use std::ops::RangeBounds;

use common_meta_types::KVMeta;
use futures_util::stream::BoxStream;
use stream_more::KMerge;
use stream_more::StreamMore;

use crate::sm_v002::leveled_store::util;
use crate::sm_v002::marked::Marked;
use crate::state_machine::ExpireKey;

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
#[async_trait::async_trait]
pub(in crate::sm_v002) trait MapApiRO<K>: Send + Sync
where K: MapKey
{
    /// Get an entry by key.
    async fn get<Q>(&self, key: &Q) -> Marked<K::V>
    where
        K: Borrow<Q>,
        Q: Ord + Send + Sync + ?Sized;

    /// Iterate over a range of entries by keys.
    ///
    /// The returned iterator contains tombstone entries: [`Marked::TombStone`].
    async fn range<'f, Q, R>(&'f self, range: R) -> BoxStream<'f, (K, Marked<K::V>)>
    where
        K: Borrow<Q>,
        Q: Ord + Send + Sync + ?Sized,
        R: RangeBounds<Q> + Send + Sync + Clone;
}

/// Trait for using Self as an implementation of the MapApi.
pub(in crate::sm_v002) trait AsMap {
    /// Use Self as an implementation of the [`MapApiRO`] (Read-Only) interface.
    fn as_map<K: MapKey>(&self) -> &impl MapApiRO<K>
    where Self: MapApiRO<K> + Sized {
        self
    }

    /// Use Self as an implementation of the [`MapApi`] interface, allowing for mutation.
    fn as_map_mut<K: MapKey>(&mut self) -> &mut impl MapApi<K>
    where Self: MapApi<K> + Sized {
        self
    }

    fn str_map(&self) -> &impl MapApiRO<String>
    where Self: MapApiRO<String> + Sized {
        self
    }

    fn expire_map(&self) -> &impl MapApiRO<ExpireKey>
    where Self: MapApiRO<ExpireKey> + Sized {
        self
    }

    fn str_map_mut(&mut self) -> &mut impl MapApi<String>
    where Self: MapApi<String> + Sized {
        self
    }

    fn expire_map_mut(&mut self) -> &mut impl MapApi<ExpireKey>
    where Self: MapApi<ExpireKey> + Sized {
        self
    }
}

impl<T> AsMap for T {}

/// Provide a read-write key-value map API set, used to access state machine data.
#[async_trait::async_trait]
pub(in crate::sm_v002) trait MapApi<K>: MapApiRO<K>
where K: MapKey
{
    /// Set an entry and returns the old value and the new value.
    async fn set(
        &mut self,
        key: K,
        value: Option<(K::V, Option<KVMeta>)>,
    ) -> (Marked<K::V>, Marked<K::V>);
}

pub(in crate::sm_v002) struct MapApiExt;

impl MapApiExt {
    /// Update only the meta associated to an entry and keeps the value unchanged.
    /// If the entry does not exist, nothing is done.
    pub(in crate::sm_v002) async fn update_meta<K, T>(
        s: &mut T,
        key: K,
        meta: Option<KVMeta>,
    ) -> (Marked<K::V>, Marked<K::V>)
    where
        K: MapKey,
        T: MapApi<K>,
    {
        //
        let got = s.get(&key).await;
        if got.is_tomb_stone() {
            return (got.clone(), got.clone());
        }

        // Safe unwrap(), got is Normal
        let (v, _) = got.unpack_ref().unwrap();

        s.set(key, Some((v.clone(), meta))).await
    }

    /// Update only the value and keeps the meta unchanged.
    /// If the entry does not exist, create one.
    #[allow(dead_code)]
    pub(in crate::sm_v002) async fn upsert_value<K, T>(
        s: &mut T,
        key: K,
        value: K::V,
    ) -> (Marked<K::V>, Marked<K::V>)
    where
        K: MapKey,
        T: MapApi<K>,
    {
        let got = s.get(&key).await;

        let meta = if let Some((_, meta)) = got.unpack_ref() {
            meta
        } else {
            None
        };

        s.set(key, Some((value, meta.cloned()))).await
    }
}

/// Get a key from multi levels data.
///
/// Returns the first non-tombstone entry.
pub(in crate::sm_v002) async fn compacted_get<'d, K, Q, L>(
    key: &Q,
    levels: impl IntoIterator<Item = &'d L>,
) -> Marked<K::V>
where
    K: MapKey,
    K: Borrow<Q>,
    Q: Ord + Send + Sync + ?Sized,
    L: MapApiRO<K> + 'static,
{
    for lvl in levels {
        let got = lvl.get(key).await;
        if !got.not_found() {
            return got;
        }
    }
    Marked::empty()
}

/// Iterate over a range of entries by keys from multi levels.
///
/// The returned iterator contains at most one entry for each key.
/// There could be tombstone entries: [`Marked::TombStone`]
pub(in crate::sm_v002) async fn compacted_range<'d, K, Q, R, L>(
    range: R,
    levels: impl IntoIterator<Item = &'d L>,
) -> BoxStream<'d, (K, Marked<K::V>)>
where
    K: MapKey,
    K: Borrow<Q>,
    R: RangeBounds<Q> + Clone + Send + Sync,
    Q: Ord + Send + Sync + ?Sized,
    L: MapApiRO<K> + 'static,
{
    let mut kmerge = KMerge::by(util::by_key_seq);

    for lvl in levels {
        let strm = lvl.range(range.clone()).await;
        kmerge = kmerge.merge(strm);
    }

    // Merge entries with the same key, keep the one with larger internal-seq
    let m = kmerge.coalesce(util::choose_greater);

    let strm: BoxStream<'_, (K, Marked<K::V>)> = Box::pin(m);
    strm
}

#[cfg(test)]
mod tests {
    use futures_util::StreamExt;

    use crate::sm_v002::leveled_store::level::Level;
    use crate::sm_v002::leveled_store::map_api::compacted_get;
    use crate::sm_v002::leveled_store::map_api::compacted_range;
    use crate::sm_v002::leveled_store::map_api::MapApi;
    use crate::sm_v002::marked::Marked;

    #[tokio::test]
    async fn test_compacted_get() -> anyhow::Result<()> {
        let mut l0 = Level::default();
        l0.set(s("a"), Some((b("a"), None))).await;

        let mut l1 = l0.new_level();
        l1.set(s("a"), None).await;

        let l2 = l1.new_level();

        let got = compacted_get::<String, _, _>(&s("a"), [&l0, &l1, &l2]).await;
        assert_eq!(got, Marked::new_normal(1, b("a")));

        let got = compacted_get::<String, _, _>(&s("a"), [&l2, &l1, &l0]).await;
        assert_eq!(got, Marked::new_tomb_stone(1));

        let got = compacted_get::<String, _, _>(&s("a"), [&l1, &l0]).await;
        assert_eq!(got, Marked::new_tomb_stone(1));

        let got = compacted_get::<String, _, _>(&s("a"), [&l2, &l0]).await;
        assert_eq!(got, Marked::new_normal(1, b("a")));
        Ok(())
    }

    #[tokio::test]
    async fn test_compacted_range() -> anyhow::Result<()> {
        // ```
        // l2 |    b
        // l1 | a*    c*
        // l0 | a  b
        // ```
        let mut l0 = Level::default();
        l0.set(s("a"), Some((b("a"), None))).await;
        l0.set(s("b"), Some((b("b"), None))).await;

        let mut l1 = l0.new_level();
        l1.set(s("a"), None).await;
        l1.set(s("c"), None).await;

        let mut l2 = l1.new_level();
        l2.set(s("b"), Some((b("b2"), None))).await;

        let got = compacted_range::<String, String, _, _>(&s("").., [&l2, &l1, &l0]).await;
        let got = got.collect::<Vec<_>>().await;
        assert_eq!(got, vec![
            //
            (s("a"), Marked::new_tomb_stone(2)),
            (s("b"), Marked::new_normal(3, b("b2"))),
            (s("c"), Marked::new_tomb_stone(2)),
        ]);

        let got = compacted_range::<String, String, _, _>(&s("b").., [&l2, &l1, &l0]).await;
        let got = got.collect::<Vec<_>>().await;
        assert_eq!(got, vec![
            //
            (s("b"), Marked::new_normal(3, b("b2"))),
            (s("c"), Marked::new_tomb_stone(2)),
        ]);

        Ok(())
    }

    fn s(x: impl ToString) -> String {
        x.to_string()
    }

    fn b(x: impl ToString) -> Vec<u8> {
        x.to_string().as_bytes().to_vec()
    }
}
