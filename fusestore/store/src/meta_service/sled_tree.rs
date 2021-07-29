// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt::Debug;
use std::fmt::Display;
use std::marker::PhantomData;
use std::ops::RangeBounds;

use common_exception::ErrorCode;
use common_exception::ToErrorCode;
use common_tracing::tracing;

use crate::meta_service::sled_serde::SledOrderedSerde;
use crate::meta_service::sled_serde::SledRangeSerde;
use crate::meta_service::SledSerde;

/// Extract key from a value of sled tree that includes its key.
pub trait SledValueToKey<K> {
    fn to_key(&self) -> K;
}

/// SledTree is a wrapper of sled::Tree that provides typed access.
/// The key type `K` must be serializable with order preserved, i.e. impl trait `SledOrderedSerde`.
/// The value type `V` can be any serialize impl, i.e. for most cases, to impl trait `SledSerde`.
pub struct SledTree<K: SledOrderedSerde + Display + Debug, V: SledSerde> {
    pub name: String,
    pub(crate) tree: sled::Tree,
    phantom_k: PhantomData<K>,
    phantom_v: PhantomData<V>,
}

impl<K: SledOrderedSerde + Display + Debug, V: SledSerde> SledTree<K, V> {
    /// Open SledTree
    pub async fn open<N: AsRef<[u8]> + Display>(
        db: &sled::Db,
        tree_name: N,
    ) -> common_exception::Result<Self> {
        let t = db
            .open_tree(&tree_name)
            .map_err_to_code(ErrorCode::MetaStoreDamaged, || {
                format!("open tree: {}", tree_name)
            })?;

        let rl = SledTree {
            name: format!("{}", tree_name),
            tree: t,
            phantom_k: PhantomData,
            phantom_v: PhantomData,
        };
        Ok(rl)
    }

    /// Return true if the tree contains the key.
    pub fn contains_key(&self, key: &K) -> common_exception::Result<bool> {
        let got = self
            .tree
            .contains_key(key.ser()?)
            .map_err_to_code(ErrorCode::MetaStoreDamaged, || {
                format!("contains_key: {}:{}", self.name, key)
            })?;

        Ok(got)
    }

    /// Retrieve the value of key.
    pub fn get(&self, key: &K) -> common_exception::Result<Option<V>> {
        let got = self
            .tree
            .get(key.ser()?)
            .map_err_to_code(ErrorCode::MetaStoreDamaged, || {
                format!("get: {}:{}", self.name, key)
            })?;

        let v = match got {
            None => None,
            Some(v) => Some(V::de(&v)?),
        };

        Ok(v)
    }

    /// Retrieve the last key value pair.
    pub fn last(&self) -> common_exception::Result<Option<(K, V)>> {
        let last = self
            .tree
            .last()
            .map_err_to_code(ErrorCode::MetaStoreDamaged, || {
                format!("read last: {}", self.name)
            })?;

        let kv = match last {
            Some((k, v)) => {
                let key = K::de(&k)?;
                let value = V::de(&v)?;
                Some((key, value))
            }
            None => None,
        };

        Ok(kv)
    }

    /// Delete kvs that are in `range`.
    #[tracing::instrument(level = "debug", skip(self, range))]
    pub async fn range_delete<R>(&self, range: R, flush: bool) -> common_exception::Result<()>
    where R: RangeBounds<K> {
        let mut batch = sled::Batch::default();

        // Convert K range into sled::IVec range
        let sled_range = range.ser()?;

        let range_mes = self.range_message(&range);

        for item in self.tree.range(sled_range) {
            let (k, _) = item.map_err_to_code(ErrorCode::MetaStoreDamaged, || {
                format!("range_delete: {}", range_mes,)
            })?;
            batch.remove(k);
        }

        self.tree
            .apply_batch(batch)
            .map_err_to_code(ErrorCode::MetaStoreDamaged, || {
                format!("batch delete: {}", range_mes,)
            })?;

        if flush {
            self.tree
                .flush_async()
                .await
                .map_err_to_code(ErrorCode::MetaStoreDamaged, || {
                    format!("flush range delete: {}", range_mes,)
                })?;
        }

        Ok(())
    }

    /// Get keys in `range`
    pub fn range_keys<R>(&self, range: R) -> common_exception::Result<Vec<K>>
    where R: RangeBounds<K> {
        // TODO(xp): pre alloc vec space
        let mut res = vec![];

        let range_mes = self.range_message(&range);

        // Convert K range into sled::IVec range
        let range = range.ser()?;
        for item in self.tree.range(range) {
            let (k, _) = item.map_err_to_code(ErrorCode::MetaStoreDamaged, || {
                format!("range_get: {}", range_mes,)
            })?;

            let key = K::de(&k)?;
            res.push(key);
        }

        Ok(res)
    }

    /// Get values of key in `range`
    pub fn range_get<R>(&self, range: R) -> common_exception::Result<Vec<V>>
    where R: RangeBounds<K> {
        // TODO(xp): pre alloc vec space
        let mut res = vec![];

        let range_mes = self.range_message(&range);

        // Convert K range into sled::IVec range
        let range = range.ser()?;
        for item in self.tree.range(range) {
            let (_, v) = item.map_err_to_code(ErrorCode::MetaStoreDamaged, || {
                format!("range_get: {}", range_mes,)
            })?;

            let ent = V::de(&v)?;
            res.push(ent);
        }

        Ok(res)
    }

    /// Append many key-values into SledTree.
    pub async fn append(&self, kvs: &[(K, V)]) -> common_exception::Result<()> {
        let mut batch = sled::Batch::default();

        for (key, value) in kvs.iter() {
            let k = K::ser(key)?;
            let v = V::ser(value)?;

            batch.insert(k, v);
        }

        self.tree
            .apply_batch(batch)
            .map_err_to_code(ErrorCode::MetaStoreDamaged, || "batch append")?;

        self.tree
            .flush_async()
            .await
            .map_err_to_code(ErrorCode::MetaStoreDamaged, || "flush append")?;

        Ok(())
    }
    /// Append many values into SledTree.
    /// This could be used in cases the key is included in value and a value should impl trait `IntoKey` to retrieve the key from a value.
    #[tracing::instrument(level = "debug", skip(self, values))]
    pub async fn append_values(&self, values: &[V]) -> common_exception::Result<()>
    where V: SledValueToKey<K> {
        let mut batch = sled::Batch::default();

        for value in values.iter() {
            let key: K = value.to_key();

            let k = key.ser()?;
            let v = V::ser(value)?;

            batch.insert(k, v);
        }

        self.tree
            .apply_batch(batch)
            .map_err_to_code(ErrorCode::MetaStoreDamaged, || "batch append_values")?;

        {
            let span = tracing::span!(tracing::Level::DEBUG, "flush-append");
            let _ent = span.enter();

            self.tree
                .flush_async()
                .await
                .map_err_to_code(ErrorCode::MetaStoreDamaged, || "flush append_values")?;
        }

        Ok(())
    }

    /// Insert a single kv.
    /// Returns the last value if it is set.
    #[tracing::instrument(level = "debug", skip(self, value))]
    pub async fn insert(&self, key: &K, value: &V) -> common_exception::Result<Option<V>> {
        let k = key.ser()?;
        let v = value.ser()?;

        let prev = self
            .tree
            .insert(k, v)
            .map_err_to_code(ErrorCode::MetaStoreDamaged, || {
                format!("insert_value {}", key)
            })?;

        let prev = match prev {
            None => None,
            Some(x) => Some(V::de(x)?),
        };

        {
            let span = tracing::span!(tracing::Level::DEBUG, "flush-insert");
            let _ent = span.enter();

            self.tree
                .flush_async()
                .await
                .map_err_to_code(ErrorCode::MetaStoreDamaged, || {
                    format!("flush insert_value {}", key)
                })?;
        }

        Ok(prev)
    }

    /// Insert a single kv, Retrieve the key from value.
    #[tracing::instrument(level = "debug", skip(self, value))]
    pub async fn insert_value(&self, value: &V) -> common_exception::Result<Option<V>>
    where V: SledValueToKey<K> {
        let key = value.to_key();
        self.insert(&key, value).await
    }

    /// Build a string describing the range for a range operation.
    fn range_message<R>(&self, range: &R) -> String
    where R: RangeBounds<K> {
        format!(
            "{}:{:?} to {:?}",
            self.name,
            range.start_bound(),
            range.end_bound()
        )
    }
}
