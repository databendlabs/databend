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

use crate::marked::Marked;
use crate::state_machine::ExpireKey;

pub trait MapKeyEncode {
    /// PREFIX is the prefix of the key used to define key space in the on-disk storage.
    const PREFIX: &'static str;

    fn encode<W: Write>(&self, w: W) -> Result<(), fmt::Error>;
}

pub trait MapKeyDecode: Sized {
    fn decode(buf: &str) -> Result<Self, io::Error>;
}

/// A Marked value type of key type.
pub(crate) type MarkedOf<K> = Marked<<K as MapKey<KVMeta>>::V>;

/// A key-value pair used in a map.
pub(crate) type MapKV<K> = (K, MarkedOf<K>);

/// A stream of result of key-value returned by `range()`.
pub(crate) type KVResultStream<K> = IOResultStream<MapKV<K>>;

/// Trait for using Self as an implementation of the MapApi.
#[allow(dead_code)]
pub trait AsMap {
    /// Use Self as an implementation of the [`MapApiRO`] (Read-Only) interface.
    fn as_map<K: MapKey<KVMeta>>(&self) -> &impl MapApiRO<K, KVMeta>
    where Self: MapApiRO<K, KVMeta> + Sized {
        self
    }

    /// Use Self as an implementation of the [`MapApi`] interface, allowing for mutation.
    fn as_map_mut<K: MapKey<KVMeta>>(&mut self) -> &mut impl MapApi<K, KVMeta>
    where Self: MapApi<K, KVMeta> + Sized {
        self
    }

    fn str_map(&self) -> &impl MapApiRO<String, KVMeta>
    where Self: MapApiRO<String, KVMeta> + Sized {
        self
    }

    fn expire_map(&self) -> &impl MapApiRO<ExpireKey, KVMeta>
    where Self: MapApiRO<ExpireKey, KVMeta> + Sized {
        self
    }

    fn str_map_mut(&mut self) -> &mut impl MapApi<String, KVMeta>
    where Self: MapApi<String, KVMeta> + Sized {
        self
    }

    fn expire_map_mut(&mut self) -> &mut impl MapApi<ExpireKey, KVMeta>
    where Self: MapApi<ExpireKey, KVMeta> + Sized {
        self
    }
}

impl<T> AsMap for T {}

pub(crate) struct MapApiExt;

impl MapApiExt {
    /// Update only the meta associated to an entry and keeps the value unchanged.
    /// If the entry does not exist, nothing is done.
    pub(crate) async fn update_meta<K, T>(
        s: &mut T,
        key: K,
        meta: Option<KVMeta>,
    ) -> Result<BeforeAfter<MarkedOf<K>>, io::Error>
    where
        K: MapKey<KVMeta>,
        K: MapKeyEncode,
        T: MapApi<K, KVMeta>,
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
    ) -> Result<BeforeAfter<MarkedOf<K>>, io::Error>
    where
        K: MapKey<KVMeta>,
        K: MapKeyEncode,
        T: MapApi<K, KVMeta>,
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
