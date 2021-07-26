// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt::Display;
use std::ops::Bound;
use std::ops::RangeBounds;

use common_exception::ErrorCode;
use common_exception::ToErrorCode;

use crate::meta_service::sledkv::SledKV;
use crate::meta_service::SledValueToKey;

/// SledVarTypeTree is a wrapper of sled::Tree that provides access of more than one key-value
/// types.
/// A `SledKVType` defines a key-value type to be stored.
/// The key type `K` must be serializable with order preserved, i.e. impl trait `SledOrderedSerde`.
/// The value type `V` can be any serialize impl, i.e. for most cases, to impl trait `SledSerde`.
pub struct SledVarTypeTree {
    pub name: String,
    pub(crate) tree: sled::Tree,
}

impl SledVarTypeTree {
    /// Open SledVarTypeTree
    pub async fn open<N: AsRef<[u8]> + Display>(
        db: &sled::Db,
        tree_name: N,
    ) -> common_exception::Result<Self> {
        let t = db
            .open_tree(&tree_name)
            .map_err_to_code(ErrorCode::MetaStoreDamaged, || {
                format!("open tree: {}", tree_name)
            })?;

        let rl = SledVarTypeTree {
            name: format!("{}", tree_name),
            tree: t,
        };
        Ok(rl)
    }

    /// Return true if the tree contains the key.
    pub fn contains_key<KV: SledKV>(&self, key: &KV::K) -> common_exception::Result<bool>
    where KV: SledKV {
        let got = self
            .tree
            .contains_key(KV::serialize_key(key)?)
            .map_err_to_code(ErrorCode::MetaStoreDamaged, || {
                format!("contains_key: {}:{}", self.name, key)
            })?;

        Ok(got)
    }

    /// Retrieve the value of key.
    pub fn get<KV: SledKV>(&self, key: &KV::K) -> common_exception::Result<Option<KV::V>>
    where KV: SledKV {
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
    where KV: SledKV {
        // TODO(xp): last should be limited to the value range

        let range = (
            Bound::Unbounded,
            KV::serialize_bound(Bound::Unbounded, "right")?,
        );

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

    /// Delete kvs that are in `range`.
    pub async fn range_delete<KV, R>(&self, range: R, flush: bool) -> common_exception::Result<()>
    where
        KV: SledKV,
        R: RangeBounds<KV::K>,
    {
        let mut batch = sled::Batch::default();

        // Convert K range into sled::IVec range
        let sled_range = KV::serialize_range(&range)?;

        let range_mes = self.range_message::<KV, _>(&range);

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
    pub fn range_keys<KV, R>(&self, range: R) -> common_exception::Result<Vec<KV::K>>
    where
        KV: SledKV,
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

    /// Get values of key in `range`
    pub fn range_get<KV, R>(&self, range: R) -> common_exception::Result<Vec<KV::V>>
    where
        KV: SledKV,
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

    /// Append many key-values into SledVarTypeTree.
    pub async fn append<KV>(&self, kvs: &[(KV::K, KV::V)]) -> common_exception::Result<()>
    where KV: SledKV {
        let mut batch = sled::Batch::default();

        for (key, value) in kvs.iter() {
            let k = KV::serialize_key(key)?;
            let v = KV::serialize_value(value)?;

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

    /// Append many values into SledVarTypeTree.
    /// This could be used in cases the key is included in value and a value should impl trait `IntoKey` to retrieve the key from a value.
    pub async fn append_values<KV>(&self, values: &[KV::V]) -> common_exception::Result<()>
    where
        KV: SledKV,
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

        self.tree
            .flush_async()
            .await
            .map_err_to_code(ErrorCode::MetaStoreDamaged, || "flush append_values")?;

        Ok(())
    }

    /// Insert a single kv.
    /// Returns the last value if it is set.
    pub async fn insert<KV>(
        &self,
        key: &KV::K,
        value: &KV::V,
    ) -> common_exception::Result<Option<KV::V>>
    where
        KV: SledKV,
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

        self.tree
            .flush_async()
            .await
            .map_err_to_code(ErrorCode::MetaStoreDamaged, || {
                format!("flush insert_value {}", key)
            })?;

        Ok(prev)
    }

    /// Insert a single kv, Retrieve the key from value.
    pub async fn insert_value<KV>(&self, value: &KV::V) -> common_exception::Result<Option<KV::V>>
    where
        KV: SledKV,
        KV::V: SledValueToKey<KV::K>,
    {
        let key = value.to_key();
        self.insert::<KV>(&key, value).await
    }

    /// Build a string describing the range for a range operation.
    fn range_message<KV, R>(&self, range: &R) -> String
    where
        KV: SledKV,
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
}
