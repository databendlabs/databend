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
use std::collections::BTreeMap;
use std::ops::RangeBounds;

use common_meta_types::KVMeta;
use futures_util::stream::BoxStream;
use futures_util::StreamExt;

use crate::sm_v002::leveled_store::map_api::MapApi;
use crate::sm_v002::leveled_store::map_api::MapApiRO;
use crate::sm_v002::leveled_store::sys_data::SysData;
use crate::sm_v002::leveled_store::sys_data_api::SysDataApiRO;
use crate::sm_v002::marked::Marked;
use crate::state_machine::ExpireKey;

/// A single level of state machine data.
///
/// State machine data is composed of multiple levels.
#[derive(Debug, Default)]
pub struct LevelData {
    /// System data(non-user data).
    sys_data: SysData,

    /// Generic Key-value store.
    kv: BTreeMap<String, Marked<Vec<u8>>>,

    /// The expiration queue of generic kv.
    expire: BTreeMap<ExpireKey, Marked<String>>,
}

impl LevelData {
    /// Create a new level that is based on this level.
    pub(crate) fn new_level(&self) -> Self {
        Self {
            // sys data are cloned
            sys_data: self.sys_data.clone(),

            // Large data set is referenced.
            kv: Default::default(),
            expire: Default::default(),
        }
    }

    pub(in crate::sm_v002) fn sys_data_mut(&mut self) -> &mut SysData {
        &mut self.sys_data
    }

    /// Replace the kv store with a new one.
    pub(in crate::sm_v002) fn replace_kv(&mut self, kv: BTreeMap<String, Marked<Vec<u8>>>) {
        self.kv = kv;
    }

    /// Replace the expire queue with a new one.
    pub(in crate::sm_v002) fn replace_expire(
        &mut self,
        expire: BTreeMap<ExpireKey, Marked<String>>,
    ) {
        self.expire = expire;
    }
}

#[async_trait::async_trait]
impl MapApiRO<String> for LevelData {
    type V = Vec<u8>;

    async fn get<Q>(&self, key: &Q) -> Marked<Self::V>
    where
        String: Borrow<Q>,
        Q: Ord + Send + Sync + ?Sized,
    {
        self.kv.get(key).cloned().unwrap_or(Marked::empty())
    }

    async fn range<'a, T, R>(&'a self, range: R) -> BoxStream<'a, (String, Marked)>
    where
        String: 'a,
        String: Borrow<T>,
        T: Ord + ?Sized,
        R: RangeBounds<T> + Send,
    {
        futures::stream::iter(self.kv.range(range).map(|(k, v)| (k.clone(), v.clone()))).boxed()
    }
}

#[async_trait::async_trait]
impl MapApi<String> for LevelData {
    async fn set(
        &mut self,
        key: String,
        value: Option<(Self::V, Option<KVMeta>)>,
    ) -> (Marked<Self::V>, Marked<Self::V>) {
        // The chance it is the bottom level is very low in a loaded system.
        // Thus we always tombstone the key if it is None.

        // dbg!("set kv", &key, &value);

        let marked = if let Some((v, meta)) = value {
            let seq = self.sys_data_mut().next_seq();
            Marked::new_normal(seq, v, meta)
        } else {
            // Do not increase the sequence number, just use the max seq for all tombstone.
            let seq = self.curr_seq();
            Marked::new_tomb_stone(seq)
        };

        let prev = MapApiRO::<String>::get(self, key.as_str()).await;
        self.kv.insert(key, marked.clone());
        (prev, marked)
    }
}

#[async_trait::async_trait]
impl MapApiRO<ExpireKey> for LevelData {
    type V = String;

    async fn get<Q>(&self, key: &Q) -> Marked<Self::V>
    where
        ExpireKey: Borrow<Q>,
        Q: Ord + Send + Sync + ?Sized,
    {
        self.expire.get(key).cloned().unwrap_or(Marked::empty())
    }

    async fn range<'a, T: ?Sized, R>(
        &'a self,
        range: R,
    ) -> BoxStream<'a, (ExpireKey, Marked<String>)>
    where
        ExpireKey: 'a,
        ExpireKey: Borrow<T>,
        T: Ord,
        R: RangeBounds<T> + Send,
    {
        let it = self
            .expire
            .range(range)
            .map(|(k, v)| (k.clone(), v.clone()));

        futures::stream::iter(it).boxed()
    }
}

#[async_trait::async_trait]
impl MapApi<ExpireKey> for LevelData {
    async fn set(
        &mut self,
        key: ExpireKey,
        value: Option<(Self::V, Option<KVMeta>)>,
    ) -> (Marked<Self::V>, Marked<Self::V>) {
        // dbg!("set expire", &key, &value);

        let seq = self.curr_seq();

        let marked = if let Some((v, meta)) = value {
            Marked::from((seq, v, meta))
        } else {
            Marked::TombStone { internal_seq: seq }
        };

        let prev = MapApiRO::<ExpireKey>::get(self, &key).await;
        self.expire.insert(key, marked.clone());
        (prev, marked)
    }
}

impl AsRef<SysData> for LevelData {
    fn as_ref(&self) -> &SysData {
        &self.sys_data
    }
}
