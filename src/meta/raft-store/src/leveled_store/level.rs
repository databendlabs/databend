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

use std::collections::BTreeMap;
use std::io;
use std::ops::RangeBounds;

use databend_common_meta_types::sys_data::SysData;
use futures_util::StreamExt;
use log::warn;
use map_api::map_api::MapApi;
use map_api::map_api_ro::MapApiRO;
use map_api::map_key::MapKey;
use map_api::BeforeAfter;
use seq_marked::SeqMarked;

use crate::leveled_store::map_api::AsMap;
use crate::leveled_store::map_api::KVResultStream;
use crate::leveled_store::map_api::SeqMarkedOf;
use crate::leveled_store::sys_data_api::SysDataApiRO;
use crate::marked::MetaValue;
use crate::state_machine::ExpireKey;
use crate::state_machine::UserKey;

/// A single level of state machine data.
///
/// State machine data is composed of multiple levels.
#[derive(Debug, Default)]
pub struct Level {
    /// System data(non-user data).
    pub(crate) sys_data: SysData,

    /// Generic Key-value store.
    pub(crate) kv: BTreeMap<UserKey, SeqMarked<MetaValue>>,

    /// The expiration queue of generic kv.
    pub(crate) expire: BTreeMap<ExpireKey, SeqMarked<String>>,
}

impl Level {
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

    pub(crate) fn sys_data_ref(&self) -> &SysData {
        &self.sys_data
    }

    pub(crate) fn sys_data_mut(&mut self) -> &mut SysData {
        &mut self.sys_data
    }

    /// Replace the kv store with a new one.
    pub(crate) fn replace_kv(&mut self, kv: BTreeMap<UserKey, SeqMarked<MetaValue>>) {
        self.kv = kv;
    }

    /// Replace the expiry queue with a new one.
    pub(crate) fn replace_expire(&mut self, expire: BTreeMap<ExpireKey, SeqMarked<String>>) {
        self.expire = expire;
    }
}

#[async_trait::async_trait]
impl MapApiRO<UserKey> for Level {
    async fn get(&self, key: &UserKey) -> Result<SeqMarked<MetaValue>, io::Error> {
        let got = self
            .kv
            .get(key)
            .cloned()
            .unwrap_or(SeqMarked::new_not_found());
        Ok(got)
    }

    async fn range<R>(&self, range: R) -> Result<KVResultStream<UserKey>, io::Error>
    where R: RangeBounds<UserKey> + Clone + Send + Sync + 'static {
        // Level is borrowed. It has to copy the result to make the returning stream static.
        let vec = self
            .kv
            .range(range)
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect::<Vec<_>>();

        if vec.len() > 1000 {
            warn!(
                "Level::<UserKey>::range() returns big range of len={}",
                vec.len()
            );
        }

        let strm = futures::stream::iter(vec).map(Ok).boxed();
        Ok(strm)
    }
}

#[async_trait::async_trait]
impl MapApi<UserKey> for Level {
    async fn set(
        &mut self,
        key: UserKey,
        value: Option<MetaValue>,
    ) -> Result<BeforeAfter<SeqMarked<MetaValue>>, io::Error> {
        // The chance it is the bottom level is very low in a loaded system.
        // Thus, we always tombstone the key if it is None.

        let marked = if let Some(meta_v) = value {
            let seq = self.sys_data_mut().next_seq();
            SeqMarked::new_normal(seq, meta_v)
        } else {
            // Do not increase the sequence number, just use the max seq for all tombstone.
            let seq = self.curr_seq();
            SeqMarked::new_tombstone(seq)
        };

        let prev = (*self).as_user_map().get(&key).await?;
        self.kv.insert(key, marked.clone());
        Ok((prev, marked))
    }
}

#[async_trait::async_trait]
impl MapApiRO<ExpireKey> for Level {
    async fn get(&self, key: &ExpireKey) -> Result<SeqMarkedOf<ExpireKey>, io::Error> {
        let got = self
            .expire
            .get(key)
            .cloned()
            .unwrap_or(SeqMarked::new_not_found());
        Ok(got)
    }

    async fn range<R>(&self, range: R) -> Result<KVResultStream<ExpireKey>, io::Error>
    where R: RangeBounds<ExpireKey> + Clone + Send + Sync + 'static {
        // Level is borrowed. It has to copy the result to make the returning stream static.
        let vec = self
            .expire
            .range(range)
            .map(|(k, v)| (*k, v.clone()))
            .collect::<Vec<_>>();

        if vec.len() > 1000 {
            warn!(
                "Level::<ExpireKey>::range() returns big range of len={}",
                vec.len()
            );
        }

        let strm = futures::stream::iter(vec).map(Ok).boxed();
        Ok(strm)
    }
}

#[async_trait::async_trait]
impl MapApi<ExpireKey> for Level {
    async fn set(
        &mut self,
        key: ExpireKey,
        value: Option<<ExpireKey as MapKey>::V>,
    ) -> Result<BeforeAfter<SeqMarkedOf<ExpireKey>>, io::Error> {
        let seq = self.curr_seq();

        let marked = if let Some(v) = value {
            SeqMarked::new_normal(seq, v)
        } else {
            SeqMarked::new_tombstone(seq)
        };

        let prev = (*self).as_expire_map().get(&key).await?;
        self.expire.insert(key, marked.clone());
        Ok((prev, marked))
    }
}

impl AsRef<SysData> for Level {
    fn as_ref(&self) -> &SysData {
        &self.sys_data
    }
}
