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

//! [`MapApi`] and [`MapApiRO`] defines the behavior of a key-value map and readonly key-value map.

use std::borrow::Borrow;
use std::fmt;
use std::io;
use std::ops::RangeBounds;

use databend_common_meta_types::KVMeta;
use futures::stream::StreamExt;
use futures_util::stream::BoxStream;
use stream_more::KMerge;
use stream_more::StreamMore;

use crate::leveled_store::util;
use crate::marked::Marked;
use crate::state_machine::ExpireKey;

/// MapKey defines the behavior of a key in a map.
///
/// It is `Clone` to let MapApi clone a range of key.
/// It is `Unpin` to let MapApi extract a key from pinned data, such as a stream.
/// And it only accepts `'static` value for simplicity.
pub(crate) trait MapKey: Clone + Ord + fmt::Debug + Send + Sync + Unpin + 'static {
    type V: MapValue;
}

/// MapValue defines the behavior of a value in a map.
///
/// It is `Clone` to let MapApi return an owned value.
/// It is `Unpin` to let MapApi extract a value from pinned data, such as a stream.
/// And it only accepts `'static` value for simplicity.
pub(crate) trait MapValue: Clone + Send + Sync + Unpin + 'static {}

/// A Marked value type of a key type.
pub(crate) type MarkedOf<K> = Marked<<K as MapKey>::V>;

/// A key-value pair used in a map.
pub(crate) type MapKV<K> = (K, MarkedOf<K>);

/// Transition from one state to another.
pub(crate) type Transition<T> = (T, T);

/// A boxed stream of io results of key-value pair.
pub(crate) type ResultStream<T> = BoxStream<'static, Result<T, io::Error>>;

/// A stream of result of key-value returned by `range()`.
pub(crate) type KVResultStream<K> = ResultStream<MapKV<K>>;

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
pub(crate) trait MapApiRO<K>: Send + Sync
where K: MapKey
{
    /// Get an entry by key.
    async fn get<Q>(&self, key: &Q) -> Result<MarkedOf<K>, io::Error>
    where
        K: Borrow<Q>,
        Q: Ord + Send + Sync + ?Sized;

    /// Iterate over a range of entries by keys.
    ///
    /// The returned iterator contains tombstone entries: [`Marked::TombStone`].
    async fn range<R>(&self, range: R) -> Result<KVResultStream<K>, io::Error>
    where R: RangeBounds<K> + Send + Sync + Clone + 'static;
}

/// Trait for using Self as an implementation of the MapApi.
#[allow(dead_code)]
pub(crate) trait AsMap {
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
pub(crate) trait MapApi<K>: MapApiRO<K>
where K: MapKey
{
    /// Set an entry and returns the old value and the new value.
    async fn set(
        &mut self,
        key: K,
        value: Option<(K::V, Option<KVMeta>)>,
    ) -> Result<Transition<MarkedOf<K>>, io::Error>;
}

pub(crate) struct MapApiExt;

impl MapApiExt {
    /// Update only the meta associated to an entry and keeps the value unchanged.
    /// If the entry does not exist, nothing is done.
    pub(crate) async fn update_meta<K, T>(
        s: &mut T,
        key: K,
        meta: Option<KVMeta>,
    ) -> Result<Transition<MarkedOf<K>>, io::Error>
    where
        K: MapKey,
        T: MapApi<K>,
    {
        //
        let got = s.get(&key).await?;
        if got.is_tombstone() {
            return Ok((got.clone(), got.clone()));
        }

        // Safe unwrap(), got is Normal
        let (v, _) = got.unpack_ref().unwrap();

        s.set(key, Some((v.clone(), meta))).await
    }

    /// Update only the value and keeps the meta unchanged.
    /// If the entry does not exist, create one.
    #[allow(dead_code)]
    pub(crate) async fn upsert_value<K, T>(
        s: &mut T,
        key: K,
        value: K::V,
    ) -> Result<Transition<MarkedOf<K>>, io::Error>
    where
        K: MapKey,
        T: MapApi<K>,
    {
        let got = s.get(&key).await?;

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
///
/// `db` is the bottom level db.
pub(crate) async fn compacted_get<'d, K, Q, L>(
    key: &Q,
    levels: impl IntoIterator<Item = &'d L>,
) -> Result<MarkedOf<K>, io::Error>
where
    K: MapKey,
    K: Borrow<Q>,
    Q: Ord + Send + Sync + ?Sized,
    L: MapApiRO<K> + 'static,
{
    for lvl in levels {
        let got = lvl.get(key).await?;
        if !got.not_found() {
            return Ok(got);
        }
    }

    Ok(Marked::empty())
}

/// Iterate over a range of entries by keys from multi levels.
///
/// The returned iterator contains at most one entry for each key.
/// There could be tombstone entries: [`Marked::TombStone`].
///
/// The `TOP` is the type of the top level.
/// The `L` is the type of immutable levels.
///
/// Because the top level is very likely to be a different type from the immutable levels, i.e., it is writable.
pub(crate) async fn compacted_range<'d, K, R, L, TOP>(
    range: R,
    top: Option<&'d TOP>,
    levels: impl IntoIterator<Item = &'d L>,
) -> Result<KVResultStream<K>, io::Error>
where
    K: MapKey,
    R: RangeBounds<K> + Clone + Send + Sync + 'static,
    L: MapApiRO<K> + 'static,
    TOP: MapApiRO<K> + 'static,
{
    let mut kmerge = KMerge::by(util::by_key_seq);

    if let Some(t) = top {
        let strm = t.range(range.clone()).await?;
        kmerge = kmerge.merge(strm);
    }

    for lvl in levels {
        let strm = lvl.range(range.clone()).await?;
        kmerge = kmerge.merge(strm);
    }

    // Merge entries with the same key, keep the one with larger internal-seq
    let coalesce = kmerge.coalesce(util::choose_greater);

    Ok(coalesce.boxed())
}

#[cfg(test)]
mod tests {

    use futures_util::TryStreamExt;

    use crate::leveled_store::immutable::Immutable;
    use crate::leveled_store::level::Level;
    use crate::leveled_store::map_api::compacted_get;
    use crate::leveled_store::map_api::compacted_range;
    use crate::leveled_store::map_api::MapApi;
    use crate::marked::Marked;

    #[tokio::test]
    async fn test_compacted_get() -> anyhow::Result<()> {
        let mut l0 = Level::default();
        l0.set(s("a"), Some((b("a"), None))).await?;

        let mut l1 = l0.new_level();
        l1.set(s("a"), None).await?;

        let l2 = l1.new_level();

        let got = compacted_get::<String, _, _>(&s("a"), [&l0, &l1, &l2]).await?;
        assert_eq!(got, Marked::new_normal(1, b("a")));

        let got = compacted_get::<String, _, _>(&s("a"), [&l2, &l1, &l0]).await?;
        assert_eq!(got, Marked::new_tombstone(1));

        let got = compacted_get::<String, _, _>(&s("a"), [&l1, &l0]).await?;
        assert_eq!(got, Marked::new_tombstone(1));

        let got = compacted_get::<String, _, _>(&s("a"), [&l2, &l0]).await?;
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
        l0.set(s("a"), Some((b("a"), None))).await?;
        l0.set(s("b"), Some((b("b"), None))).await?;
        let l0 = Immutable::new_from_level(l0);

        let mut l1 = l0.new_level();
        l1.set(s("a"), None).await?;
        l1.set(s("c"), None).await?;
        let l1 = Immutable::new_from_level(l1);

        let mut l2 = l1.new_level();
        l2.set(s("b"), Some((b("b2"), None))).await?;

        // With top level
        {
            let got = compacted_range(s("").., Some(&l2), [&l1, &l0]).await?;
            let got = got.try_collect::<Vec<_>>().await?;
            assert_eq!(got, vec![
                //
                (s("a"), Marked::new_tombstone(2)),
                (s("b"), Marked::new_normal(3, b("b2"))),
                (s("c"), Marked::new_tombstone(2)),
            ]);

            let got = compacted_range(s("b").., Some(&l2), [&l1, &l0]).await?;
            let got = got.try_collect::<Vec<_>>().await?;
            assert_eq!(got, vec![
                //
                (s("b"), Marked::new_normal(3, b("b2"))),
                (s("c"), Marked::new_tombstone(2)),
            ]);
        }

        // Without top level
        {
            let got = compacted_range::<_, _, _, Level>(s("").., None, [&l1, &l0]).await?;
            let got = got.try_collect::<Vec<_>>().await?;
            assert_eq!(got, vec![
                //
                (s("a"), Marked::new_tombstone(2)),
                (s("b"), Marked::new_normal(2, b("b"))),
                (s("c"), Marked::new_tombstone(2)),
            ]);

            let got = compacted_range::<_, _, _, Level>(s("b").., None, [&l1, &l0]).await?;
            let got = got.try_collect::<Vec<_>>().await?;
            assert_eq!(got, vec![
                //
                (s("b"), Marked::new_normal(2, b("b"))),
                (s("c"), Marked::new_tombstone(2)),
            ]);
        }

        Ok(())
    }

    fn s(x: impl ToString) -> String {
        x.to_string()
    }

    fn b(x: impl ToString) -> Vec<u8> {
        x.to_string().as_bytes().to_vec()
    }
}
