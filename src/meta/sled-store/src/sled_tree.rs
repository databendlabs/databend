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

use std::fmt::Display;
use std::marker::PhantomData;

use databend_common_meta_stoerr::MetaStorageError;
use fastrace::func_name;
use log::debug;
use sled::IVec;

use crate::SledBytesError;
use crate::SledKeySpace;

/// SledTree is a wrapper of sled::Tree that provides access of more than one key-value
/// types.
/// A `SledKVType` defines a key-value type to be stored.
/// The key type `K` must be serializable with order preserved, i.e. impl trait `SledOrderedSerde`.
/// The value type `V` can be any serialize impl, i.e. for most cases, to impl trait `SledSerde`.
#[derive(Debug, Clone)]
pub struct SledTree {
    pub name: String,

    pub tree: sled::Tree,
}

/// A key-value item stored in sled store.
pub struct SledItem<KV: SledKeySpace> {
    key: IVec,
    value: IVec,
    _p: PhantomData<KV>,
}

impl<KV: SledKeySpace> SledItem<KV> {
    pub fn new(key: IVec, value: IVec) -> Self {
        //
        Self {
            key,
            value,
            _p: Default::default(),
        }
    }

    pub fn key(&self) -> Result<KV::K, SledBytesError> {
        KV::deserialize_key(&self.key)
    }

    pub fn value(&self) -> Result<KV::V, SledBytesError> {
        KV::deserialize_value(&self.value)
    }

    pub fn kv(&self) -> Result<(KV::K, KV::V), SledBytesError> {
        let k = self.key()?;
        let v = self.value()?;
        Ok((k, v))
    }
}

#[allow(clippy::type_complexity)]
impl SledTree {
    /// Open SledTree
    pub fn open<N: AsRef<[u8]> + Display>(
        db: &sled::Db,
        tree_name: N,
    ) -> Result<Self, MetaStorageError> {
        // During testing, every tree name must be unique.
        if cfg!(test) {
            let x = tree_name.as_ref();
            let x = &x[0..5];
            assert_eq!(x, b"test-");
        }
        let t = db.open_tree(&tree_name)?;

        debug!("SledTree opened tree: {}", tree_name);

        let rl = SledTree {
            name: tree_name.to_string(),
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

    /// Returns a vec of key-value paris:
    pub fn export(&self) -> Result<Vec<Vec<Vec<u8>>>, std::io::Error> {
        let it = self.tree.iter();

        let mut kvs = Vec::new();

        for rkv in it {
            let (k, v) = rkv?;

            kvs.push(vec![k.to_vec(), v.to_vec()]);
        }

        Ok(kvs)
    }

    /// Retrieve the value of key.
    pub(crate) fn get<KV: SledKeySpace>(
        &self,
        key: &KV::K,
    ) -> Result<Option<KV::V>, MetaStorageError> {
        let got = self.tree.get(KV::serialize_key(key)?)?;

        let v = match got {
            None => None,
            Some(v) => Some(KV::deserialize_value(v)?),
        };

        Ok(v)
    }

    /// Remove a key without returning the previous value.
    ///
    /// Just return the size of the removed value if the key is removed.
    pub(crate) async fn remove_no_return<KV>(
        &self,
        key: &KV::K,
        flush: bool,
    ) -> Result<Option<usize>, MetaStorageError>
    where
        KV: SledKeySpace,
    {
        let k = KV::serialize_key(key)?;

        let prev = self.tree.remove(k)?;

        let removed = prev.map(|x| x.len());

        self.flush_async(flush).await?;

        Ok(removed)
    }

    #[fastrace::trace]
    async fn flush_async(&self, flush: bool) -> Result<(), MetaStorageError> {
        debug!("{}: flush: {}", func_name!(), flush);

        if flush {
            self.tree.flush_async().await?;
        }
        debug!("{}: flush: {} Done", func_name!(), flush);
        Ok(())
    }
}

/// It borrows the internal SledTree with access limited to a specified namespace `KV`.
pub struct AsKeySpace<'a, KV: SledKeySpace> {
    inner: &'a SledTree,
    phantom: PhantomData<KV>,
}

#[allow(clippy::type_complexity)]
impl<KV: SledKeySpace> AsKeySpace<'_, KV> {
    pub fn get(&self, key: &KV::K) -> Result<Option<KV::V>, MetaStorageError> {
        self.inner.get::<KV>(key)
    }

    pub async fn remove_no_return(
        &self,
        key: &KV::K,
        flush: bool,
    ) -> Result<Option<usize>, MetaStorageError> {
        self.inner.remove_no_return::<KV>(key, flush).await
    }
}
