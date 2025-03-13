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
use std::ops::RangeBounds;

use futures_util::StreamExt;
use stream_more::KMerge;
use stream_more::StreamMore;

use crate::util;
use crate::MapApiRO;
use crate::MapKey;

/// Get a key from multi levels data.
///
/// Returns the first non-tombstone entry.
///
/// `persisted` is a series of persisted on disk levels.
///
/// - `K`: key type used in a map.
/// - `M`: the value metadata type.
/// - `L`: type of the several top levels
/// - `PL`: the bottom persistent level.
pub async fn compacted_get<K, M, L, PL>(
    key: &K,
    levels: impl IntoIterator<Item = L>,
    persisted: impl IntoIterator<Item = PL>,
) -> Result<crate::MarkedOf<K, M>, io::Error>
where
    K: MapKey<M>,
    M: Unpin + Send + 'static,
    L: MapApiRO<K, M>,
    PL: MapApiRO<K, M>,
{
    for lvl in levels {
        let got = lvl.get(key).await?;
        if !got.is_not_found() {
            return Ok(got);
        }
    }

    for p in persisted {
        let got = p.get(key).await?;
        if !got.is_not_found() {
            return Ok(got);
        }
    }

    Ok(crate::Marked::empty())
}

/// Iterate over a range of entries by keys from multi levels.
///
/// The returned iterator contains at most one entry for each key.
/// There could be tombstone entries: [`Marked::TombStone`].
///
/// - `K`: key type used in a map.
/// - `M`: the value metadata type.
/// - `TOP` is the type of the top level.
/// - `L` is the type of immutable levels.
/// - `PL` is the type of the persisted level.
///
/// Because the top level is very likely to be a different type from the immutable levels, i.e., it is writable.
///
/// `persisted` is a series of persisted on disk levels that have different types.
pub async fn compacted_range<K, R, M, TOP, L, PL>(
    range: R,
    top: Option<&TOP>,
    levels: impl IntoIterator<Item = L>,
    persisted: impl IntoIterator<Item = PL>,
) -> Result<crate::KVResultStream<K, M>, io::Error>
where
    K: MapKey<M>,
    M: Unpin + Send + 'static,
    R: RangeBounds<K> + Clone + Send + Sync + 'static,
    TOP: MapApiRO<K, M> + 'static,
    L: MapApiRO<K, M>,
    PL: MapApiRO<K, M>,
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

    for p in persisted {
        let strm = p.range(range.clone()).await?;
        kmerge = kmerge.merge(strm);
    }

    // Merge entries with the same key, keep the one with larger internal-seq
    let coalesce = kmerge.coalesce(util::merge_kv_results);

    Ok(coalesce.boxed())
}

#[cfg(test)]
mod tests {

    use futures_util::TryStreamExt;

    use crate::compact::compacted_get;
    use crate::compact::compacted_range;
    use crate::impls::immutable::Immutable;
    use crate::impls::level::Level;
    use crate::marked::Marked;
    use crate::MapApi;

    #[tokio::test]
    async fn test_compacted_get() -> anyhow::Result<()> {
        let mut l0 = Level::default();
        l0.set(s("a"), Some((b("a"), None))).await?;

        let mut l1 = l0.new_level();
        l1.set(s("a"), None).await?;

        let l2 = l1.new_level();

        let got = compacted_get::<String, _, _, Level>(&s("a"), [&l0, &l1, &l2], []).await?;
        assert_eq!(got, Marked::new_normal(1, b("a")));

        let got = compacted_get::<String, _, _, Level>(&s("a"), [&l2, &l1, &l0], []).await?;
        assert_eq!(got, Marked::new_tombstone(1));

        let got = compacted_get::<String, _, _, Level>(&s("a"), [&l1, &l0], []).await?;
        assert_eq!(got, Marked::new_tombstone(1));

        let got = compacted_get::<String, _, _, Level>(&s("a"), [&l2, &l0], []).await?;
        assert_eq!(got, Marked::new_normal(1, b("a")));
        Ok(())
    }

    #[tokio::test]
    async fn test_compacted_get_with_persisted_levels() -> anyhow::Result<()> {
        let mut l0 = Level::default();
        l0.set(s("a"), Some((b("a"), None))).await?;

        let mut l1 = l0.new_level();
        l1.set(s("a"), None).await?;

        let l2 = l1.new_level();

        let mut l3 = l2.new_level();
        l3.set(s("a"), Some((b("A"), None))).await?;

        let got = compacted_get::<String, _, _, Level>(&s("a"), [&l0, &l1, &l2], []).await?;
        assert_eq!(got, Marked::new_normal(1, b("a")));

        let got = compacted_get::<String, _, _, Level>(&s("a"), [&l2, &l1, &l0], []).await?;
        assert_eq!(got, Marked::new_tombstone(1));

        let got = compacted_get::<String, _, _, &Level>(&s("a"), [&l2], [&l3]).await?;
        assert_eq!(got, Marked::new_normal(2, b("A")));

        let got = compacted_get::<String, _, _, &Level>(&s("a"), [&l2], [&l2, &l3]).await?;
        assert_eq!(got, Marked::new_normal(2, b("A")));
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
            let got =
                compacted_range::<_, _, _, _, _, Level>(s("").., Some(&l2), [&l1, &l0], []).await?;
            let got = got.try_collect::<Vec<_>>().await?;
            assert_eq!(got, vec![
                //
                (s("a"), Marked::new_tombstone(2)),
                (s("b"), Marked::new_normal(3, b("b2"))),
                (s("c"), Marked::new_tombstone(2)),
            ]);

            let got = compacted_range::<_, _, _, _, _, Level>(s("b").., Some(&l2), [&l1, &l0], [])
                .await?;
            let got = got.try_collect::<Vec<_>>().await?;
            assert_eq!(got, vec![
                //
                (s("b"), Marked::new_normal(3, b("b2"))),
                (s("c"), Marked::new_tombstone(2)),
            ]);
        }

        // Without top level
        {
            let got =
                compacted_range::<_, _, _, Level, _, Level>(s("").., None, [&l1, &l0], []).await?;
            let got = got.try_collect::<Vec<_>>().await?;
            assert_eq!(got, vec![
                //
                (s("a"), Marked::new_tombstone(2)),
                (s("b"), Marked::new_normal(2, b("b"))),
                (s("c"), Marked::new_tombstone(2)),
            ]);

            let got =
                compacted_range::<_, _, _, Level, _, Level>(s("b").., None, [&l1, &l0], []).await?;
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
