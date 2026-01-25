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

use std::fmt;
use std::fmt::Write;
use std::io;

pub use map_api::BeforeAfter;
pub use map_api::IOResultStream;
pub use map_api::map_key::MapKey;
pub use map_api::map_value::MapValue;
use map_api::mvcc;
use seq_marked::SeqMarked;
use state_machine_api::KVMeta;
use state_machine_api::MetaValue;
use state_machine_api::UserKey;

use crate::leveled_store::leveled_map::LeveledMap;

pub type MapKeyPrefix = &'static str;

pub trait MapKeyEncode {
    /// PREFIX is the prefix of the key used to define key space in the on-disk storage.
    const PREFIX: MapKeyPrefix;

    fn prefix(&self) -> MapKeyPrefix {
        Self::PREFIX
    }

    fn encode<W: Write>(&self, w: W) -> Result<(), fmt::Error>;
}

pub trait MapKeyDecode: Sized {
    fn decode(buf: &str) -> Result<Self, io::Error>;
}

pub(crate) struct MapApiHelper;

impl MapApiHelper {
    #[allow(dead_code)]
    pub(crate) async fn update_meta_on_leveled_map(
        s: &mut LeveledMap,
        key: UserKey,
        meta: Option<KVMeta>,
    ) -> Result<BeforeAfter<SeqMarked<MetaValue>>, io::Error> {
        let mut view = s.to_view();

        let got = Self::update_meta(&mut view, key, meta).await?;

        view.commit().await?;

        Ok(got)
    }

    /// Update only the meta associated to an entry and keeps the value unchanged.
    /// If the entry does not exist, nothing is done.
    pub(crate) async fn update_meta<T>(
        s: &mut T,
        key: UserKey,
        meta: Option<KVMeta>,
    ) -> Result<BeforeAfter<SeqMarked<MetaValue>>, io::Error>
    where
        T: mvcc::ScopedSet<UserKey, MetaValue> + Send + Sync + 'static,
    {
        let got = s.get(key.clone()).await?;
        if got.is_tombstone() {
            return Ok((got.clone(), got.clone()));
        }

        // Safe unwrap(), got is Normal
        let (_meta, v) = got.into_data().unwrap();

        s.fetch_and_set(key, Some((meta, v))).await
    }
}
