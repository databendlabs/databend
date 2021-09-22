// Copyright 2020 Datafuse Labs.
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

use std::fmt::Display;
use std::marker::PhantomData;
use std::ops::Bound;
use std::ops::RangeBounds;

use common_exception::ErrorCode;
use common_exception::ToErrorCode;
use common_tracing::tracing;

use crate::sled_store::sled_key_space::SledKeySpace;

/// Extract key from a value of sled tree that includes its key.
pub trait SledValueToKey<K> {
    fn to_key(&self) -> K;
}

/// SledTree is a wrapper of sled::Tree that provides access of more than one key-value
/// types.
/// A `SledKVType` defines a key-value type to be stored.
/// The key type `K` must be serializable with order preserved, i.e. impl trait `SledOrderedSerde`.
/// The value type `V` can be any serialize impl, i.e. for most cases, to impl trait `SledSerde`.
#[derive(Debug, Clone)]
pub struct SledTree {
    pub name: String,

    /// Whether to fsync after an write operation.
    /// With sync==false, it WONT fsync even when user tell it to sync.
    /// This is only used for testing when fsync is quite slow.
    /// E.g. File::sync_all takes 10 ~ 30 ms on a Mac.
    /// See: https://github.com/drmingdrmer/sledtest/blob/500929ab0b89afe547143a38fde6fe85d88f1f80/src/ben_sync.rs
    sync: bool,

    pub(crate) tree: sled::Tree,
}

impl SledTree {
    /// Open SledTree
    pub fn open<N: AsRef<[u8]> + Display>(
        db: &sled::Db,
        tree_name: N,
        sync: bool,
    ) -> common_exception::Result<Self> {
        // During testing, every tree name must be unique.
        if cfg!(test) {
            let x = tree_name.as_ref();
            let x = &x[0..5];
            assert_eq!(x, b"test-");
        }
        let t = db
            .open_tree(&tree_name)
            .map_err_to_code(ErrorCode::MetaStoreDamaged, || {
                format!("open tree: {}", tree_name)
            })?;

        tracing::debug!("SledTree opened tree: {}", tree_name);

        let rl = SledTree {
            name: format!("{}", tree_name),
            sync,
            tree: t,
        };
        Ok(rl)
    }

    /// Borrows the SledTree and creates a wrapper with access limited to a specified key space `KV`.
    pub fn key_space<KV: SledKeySpace>(&self) -> AsKeySpace<KV> {
        AsKeySpace::<KV> {
            inner: self,
            phantom: PhantomData,
        }
    }

    /// Return true if the tree contains the key.
    pub fn contains_key<KV: SledKeySpace>(&self, key: &KV::K) -> common_exception::Result<bool>
    where KV: SledKeySpace {
        let got = self
            .tree
            .contains_key(KV::serialize_key(key)?)
            .map_err_to_code(ErrorCode::MetaStoreDamaged, || {
                format!("contains_key: {}:{}", self.name, key)
            })?;

        Ok(got)
    }

    pub async fn update_and_fetch<KV: SledKeySpace, F>(
        &self,
        key: &KV::K,
        mut f: F,
    ) -> common_exception::Result<Option<KV::V>>
    where
        F: FnMut(Option<KV::V>) -> Option<KV::V>,
    {
        let mes = || format!("update_and_fetch: {}", key);

        let k = KV::serialize_key(key)?;

        let res = self
            .tree
            .update_and_fetch(k, move |old| {
                let old = old.map(|o| KV::deserialize_value(o).unwrap());

                let new_val = f(old);
                new_val.map(|new_val| KV::serialize_value(&new_val).unwrap())
            })
            .map_err_to_code(ErrorCode::MetaStoreDamaged, mes)?;

        self.flush_async(true).await?;

        let value = match res {
            None => None,
            Some(v) => Some(KV::deserialize_value(v)?),
        };

        Ok(value)
    }

    /// Retrieve the value of key.
    pub fn get<KV: SledKeySpace>(&self, key: &KV::K) -> common_exception::Result<Option<KV::V>>
    where KV: SledKeySpace {
        let got = self
            .tree
            .get(KV::serialize_key(key)?)
            .map_err_to_code(ErrorCode::MetaStoreDamaged, || {
                format!("get: {}:{}", self.name, key)
            })?;

        let v = match got {
            None => None,
            Some(v) => Some(KV::deserialize_value(v)?),
        };

        Ok(v)
    }

    /// Retrieve the last key value pair.
    pub fn last<KV>(&self) -> common_exception::Result<Option<(KV::K, KV::V)>>
    where KV: SledKeySpace {
        let range = KV::serialize_range(&(Bound::Unbounded::<KV::K>, Bound::Unbounded::<KV::K>))?;

        let mut it = self.tree.range(range).rev();
        let last = it.next();
        let last = match last {
            None => {
                return Ok(None);
            }
            Some(res) => res,
        };

        let last = last.map_err_to_code(ErrorCode::MetaStoreDamaged, || "last")?;

        let (k, v) = last;
        let key = KV::deserialize_key(k)?;
        let value = KV::deserialize_value(v)?;
        Ok(Some((key, value)))
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn remove<KV>(
        &self,
        key: &KV::K,
        flush: bool,
    ) -> common_exception::Result<Option<KV::V>>
    where
        KV: SledKeySpace,
    {
        let removed = self
            .tree
            .remove(KV::serialize_key(key)?)
            .map_err_to_code(ErrorCode::MetaStoreDamaged, || format!("removed: {}", key,))?;

        self.flush_async(flush).await?;

        let removed = match removed {
            Some(x) => Some(KV::deserialize_value(x)?),
            None => None,
        };

        Ok(removed)
    }

    /// Delete kvs that are in `range`.
    #[tracing::instrument(level = "debug", skip(self, range))]
    pub async fn range_remove<KV, R>(&self, range: R, flush: bool) -> common_exception::Result<()>
    where
        KV: SledKeySpace,
        R: RangeBounds<KV::K>,
    {
        let mut batch = sled::Batch::default();

        // Convert K range into sled::IVec range
        let sled_range = KV::serialize_range(&range)?;

        let range_mes = self.range_message::<KV, _>(&range);

        for item in self.tree.range(sled_range) {
            let (k, _) = item.map_err_to_code(ErrorCode::MetaStoreDamaged, || {
                format!("range_remove: {}", range_mes,)
            })?;
            batch.remove(k);
        }

        self.tree
            .apply_batch(batch)
            .map_err_to_code(ErrorCode::MetaStoreDamaged, || {
                format!("batch remove: {}", range_mes,)
            })?;

        self.flush_async(flush).await?;

        Ok(())
    }

    /// Get keys in `range`
    pub fn range_keys<KV, R>(&self, range: R) -> common_exception::Result<Vec<KV::K>>
    where
        KV: SledKeySpace,
        R: RangeBounds<KV::K>,
    {
        let mut res = vec![];

        let range_mes = self.range_message::<KV, _>(&range);

        // Convert K range into sled::IVec range
        let range = KV::serialize_range(&range)?;
        for item in self.tree.range(range) {
            let (k, _) = item.map_err_to_code(ErrorCode::MetaStoreDamaged, || {
                format!("range_get: {}", range_mes,)
            })?;

            let key = KV::deserialize_key(k)?;
            res.push(key);
        }

        Ok(res)
    }

    /// Get key-valuess in `range`
    pub fn range_kvs<KV, R>(&self, range: R) -> common_exception::Result<Vec<(KV::K, KV::V)>>
    where
        KV: SledKeySpace,
        R: RangeBounds<KV::K>,
    {
        let mut res = vec![];

        let range_mes = self.range_message::<KV, _>(&range);

        // Convert K range into sled::IVec range
        let range = KV::serialize_range(&range)?;
        for item in self.tree.range(range) {
            let (k, v) = item.map_err_to_code(ErrorCode::MetaStoreDamaged, || {
                format!("range_get: {}", range_mes,)
            })?;

            let key = KV::deserialize_key(k)?;
            let value = KV::deserialize_value(v)?;
            res.push((key, value));
        }

        Ok(res)
    }

    /// Get key-valuess in `range`
    pub fn range<KV, R>(
        &self,
        range: R,
    ) -> common_exception::Result<
        impl DoubleEndedIterator<Item = common_exception::Result<(KV::K, KV::V)>>,
    >
    where
        KV: SledKeySpace,
        R: RangeBounds<KV::K>,
    {
        let range_mes = self.range_message::<KV, _>(&range);

        // Convert K range into sled::IVec range
        let range = KV::serialize_range(&range)?;

        let it = self.tree.range(range);
        let it = it.map(move |item| {
            let (k, v) = item.map_err_to_code(ErrorCode::MetaStoreDamaged, || {
                format!("range_get: {}", range_mes,)
            })?;

            let key = KV::deserialize_key(k)?;
            let value = KV::deserialize_value(v)?;

            Ok((key, value))
        });

        Ok(it)
    }

    /// Get key-valuess in with the same prefix
    pub fn scan_prefix<KV>(&self, prefix: &KV::K) -> common_exception::Result<Vec<(KV::K, KV::V)>>
    where KV: SledKeySpace {
        let mut res = vec![];

        let mes = || format!("scan_prefix: {}", prefix);

        let pref = KV::serialize_key(prefix)?;
        for item in self.tree.scan_prefix(pref) {
            let (k, v) = item.map_err_to_code(ErrorCode::MetaStoreDamaged, mes)?;

            let key = KV::deserialize_key(k)?;
            let value = KV::deserialize_value(v)?;
            res.push((key, value));
        }

        Ok(res)
    }

    /// Get values of key in `range`
    pub fn range_values<KV, R>(&self, range: R) -> common_exception::Result<Vec<KV::V>>
    where
        KV: SledKeySpace,
        R: RangeBounds<KV::K>,
    {
        let mut res = vec![];

        let range_mes = self.range_message::<KV, _>(&range);

        // Convert K range into sled::IVec range
        let range = KV::serialize_range(&range)?;

        for item in self.tree.range(range) {
            let (_, v) = item.map_err_to_code(ErrorCode::MetaStoreDamaged, || {
                format!("range_get: {}", range_mes,)
            })?;

            let ent = KV::deserialize_value(v)?;
            res.push(ent);
        }

        Ok(res)
    }

    /// Append many key-values into SledTree.
    pub async fn append<KV>(&self, kvs: &[(KV::K, KV::V)]) -> common_exception::Result<()>
    where KV: SledKeySpace {
        let mut batch = sled::Batch::default();

        for (key, value) in kvs.iter() {
            let k = KV::serialize_key(key)?;
            let v = KV::serialize_value(value)?;

            batch.insert(k, v);
        }

        self.tree
            .apply_batch(batch)
            .map_err_to_code(ErrorCode::MetaStoreDamaged, || "batch append")?;

        self.flush_async(true).await?;

        Ok(())
    }

    /// Append many values into SledTree.
    /// This could be used in cases the key is included in value and a value should impl trait `IntoKey` to retrieve the key from a value.
    #[tracing::instrument(level = "debug", skip(self, values))]
    pub async fn append_values<KV>(&self, values: &[KV::V]) -> common_exception::Result<()>
    where
        KV: SledKeySpace,
        KV::V: SledValueToKey<KV::K>,
    {
        let mut batch = sled::Batch::default();

        for value in values.iter() {
            let key: KV::K = value.to_key();

            let k = KV::serialize_key(&key)?;
            let v = KV::serialize_value(value)?;

            batch.insert(k, v);
        }

        self.tree
            .apply_batch(batch)
            .map_err_to_code(ErrorCode::MetaStoreDamaged, || "batch append_values")?;

        self.flush_async(true).await?;

        Ok(())
    }

    /// Insert a single kv.
    /// Returns the last value if it is set.
    #[tracing::instrument(level = "debug", skip(self, value))]
    pub async fn insert<KV>(
        &self,
        key: &KV::K,
        value: &KV::V,
    ) -> common_exception::Result<Option<KV::V>>
    where
        KV: SledKeySpace,
    {
        let k = KV::serialize_key(key)?;
        let v = KV::serialize_value(value)?;

        let prev = self
            .tree
            .insert(k, v)
            .map_err_to_code(ErrorCode::MetaStoreDamaged, || {
                format!("insert_value {}", key)
            })?;

        let prev = match prev {
            None => None,
            Some(x) => Some(KV::deserialize_value(x)?),
        };

        self.flush_async(true).await?;

        Ok(prev)
    }

    /// Insert a single kv, Retrieve the key from value.
    #[tracing::instrument(level = "debug", skip(self, value))]
    pub async fn insert_value<KV>(&self, value: &KV::V) -> common_exception::Result<Option<KV::V>>
    where
        KV: SledKeySpace,
        KV::V: SledValueToKey<KV::K>,
    {
        let key = value.to_key();
        self.insert::<KV>(&key, value).await
    }

    /// Build a string describing the range for a range operation.
    fn range_message<KV, R>(&self, range: &R) -> String
    where
        KV: SledKeySpace,
        R: RangeBounds<KV::K>,
    {
        format!(
            "{}:{}/[{:?}, {:?}]",
            self.name,
            KV::NAME,
            range.start_bound(),
            range.end_bound()
        )
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn flush_async(&self, flush: bool) -> common_exception::Result<()> {
        if flush && self.sync {
            self.tree
                .flush_async()
                .await
                .map_err_to_code(ErrorCode::MetaStoreDamaged, || "flush sled-tree")?;
        }
        Ok(())
    }
}

/// It borrows the internal SledTree with access limited to a specified namespace `KV`.
pub struct AsKeySpace<'a, KV: SledKeySpace> {
    inner: &'a SledTree,
    phantom: PhantomData<KV>,
}

impl<'a, KV: SledKeySpace> AsKeySpace<'a, KV> {
    pub fn contains_key(&self, key: &KV::K) -> common_exception::Result<bool> {
        self.inner.contains_key::<KV>(key)
    }

    pub async fn update_and_fetch<F>(
        &self,
        key: &KV::K,
        f: F,
    ) -> common_exception::Result<Option<KV::V>>
    where
        F: FnMut(Option<KV::V>) -> Option<KV::V>,
    {
        self.inner.update_and_fetch::<KV, _>(key, f).await
    }

    pub fn get(&self, key: &KV::K) -> common_exception::Result<Option<KV::V>> {
        self.inner.get::<KV>(key)
    }

    pub fn last(&self) -> common_exception::Result<Option<(KV::K, KV::V)>> {
        self.inner.last::<KV>()
    }

    pub async fn remove(
        &self,
        key: &KV::K,
        flush: bool,
    ) -> common_exception::Result<Option<KV::V>> {
        self.inner.remove::<KV>(key, flush).await
    }

    pub async fn range_remove<R>(&self, range: R, flush: bool) -> common_exception::Result<()>
    where R: RangeBounds<KV::K> {
        self.inner.range_remove::<KV, R>(range, flush).await
    }

    pub fn range_keys<R>(&self, range: R) -> common_exception::Result<Vec<KV::K>>
    where R: RangeBounds<KV::K> {
        self.inner.range_keys::<KV, R>(range)
    }

    pub fn range<R>(
        &self,
        range: R,
    ) -> common_exception::Result<
        impl DoubleEndedIterator<Item = common_exception::Result<(KV::K, KV::V)>>,
    >
    where
        R: RangeBounds<KV::K>,
    {
        self.inner.range::<KV, R>(range)
    }

    pub fn range_kvs<R>(&self, range: R) -> common_exception::Result<Vec<(KV::K, KV::V)>>
    where R: RangeBounds<KV::K> {
        self.inner.range_kvs::<KV, R>(range)
    }

    pub fn scan_prefix(&self, prefix: &KV::K) -> common_exception::Result<Vec<(KV::K, KV::V)>> {
        self.inner.scan_prefix::<KV>(prefix)
    }

    pub fn range_values<R>(&self, range: R) -> common_exception::Result<Vec<KV::V>>
    where R: RangeBounds<KV::K> {
        self.inner.range_values::<KV, R>(range)
    }

    pub async fn append(&self, kvs: &[(KV::K, KV::V)]) -> common_exception::Result<()> {
        self.inner.append::<KV>(kvs).await
    }

    pub async fn append_values(&self, values: &[KV::V]) -> common_exception::Result<()>
    where KV::V: SledValueToKey<KV::K> {
        self.inner.append_values::<KV>(values).await
    }

    pub async fn insert(
        &self,
        key: &KV::K,
        value: &KV::V,
    ) -> common_exception::Result<Option<KV::V>> {
        self.inner.insert::<KV>(key, value).await
    }

    pub async fn insert_value(&self, value: &KV::V) -> common_exception::Result<Option<KV::V>>
    where KV::V: SledValueToKey<KV::K> {
        self.inner.insert_value::<KV>(value).await
    }
}
