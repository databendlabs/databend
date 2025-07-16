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

use std::fmt;
use std::fmt::Write;
use std::io;

use databend_common_meta_types::seq_value::KVMeta;
use map_api::map_api::MapApi;
use map_api::map_api_ro::MapApiRO;
pub use map_api::map_key::MapKey;
pub use map_api::map_value::MapValue;
pub use map_api::BeforeAfter;
pub use map_api::IOResultStream;
use seq_marked::SeqMarked;

use crate::marked::MetaValue;
use crate::state_machine::ExpireKey;
use crate::state_machine::UserKey;

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

/// A Marked value type of key type.
pub(crate) type SeqMarkedOf<K> = SeqMarked<<K as MapKey>::V>;

/// A key-value pair used in a map.
pub(crate) type MapKV<K> = (K, SeqMarkedOf<K>);

/// A stream of result of key-value returned by `range()`.
pub(crate) type KVResultStream<K> = IOResultStream<MapKV<K>>;

/// Trait for using Self as an implementation of the MapApi.
#[allow(dead_code)]
pub trait AsMap {
    fn as_user_map(&self) -> &impl MapApiRO<UserKey>
    where Self: MapApiRO<UserKey> + Sized {
        self
    }

    fn as_user_map_mut(&mut self) -> &mut impl MapApi<UserKey>
    where Self: MapApi<UserKey> + Sized {
        self
    }

    fn as_expire_map(&self) -> &impl MapApiRO<ExpireKey>
    where Self: MapApiRO<ExpireKey> + Sized {
        self
    }
}

impl<T> AsMap for T {}

pub(crate) struct MapApiHelper;

impl MapApiHelper {
    /// Update only the meta associated to an entry and keeps the value unchanged.
    /// If the entry does not exist, nothing is done.
    pub(crate) async fn update_meta<T>(
        s: &mut T,
        key: UserKey,
        meta: Option<KVMeta>,
    ) -> Result<BeforeAfter<SeqMarked<MetaValue>>, io::Error>
    where
        T: MapApi<UserKey>,
    {
        let got = s.get(&key).await?;
        if got.is_tombstone() {
            return Ok((got.clone(), got.clone()));
        }

        // Safe unwrap(), got is Normal
        let (_meta, v) = got.into_data().unwrap();

        s.set(key, Some((meta, v))).await
    }
}
