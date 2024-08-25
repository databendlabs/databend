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

//! Defines application key spaces that are defined by raft-store.
//! All of the key spaces stores key-value pairs in the underlying sled db.

use databend_common_meta_sled_store::sled;
use databend_common_meta_sled_store::SledKeySpace;
use databend_common_meta_sled_store::SledOrderedSerde;
use databend_common_meta_sled_store::SledSerde;
use databend_common_meta_stoerr::MetaStorageError;
use databend_common_meta_types::seq_value::SeqV;
use databend_common_meta_types::Entry;
use databend_common_meta_types::LogId;
use databend_common_meta_types::LogIndex;
use databend_common_meta_types::Node;
use databend_common_meta_types::NodeId;
use databend_common_meta_types::SeqNum;
use serde::Deserialize;
use serde::Serialize;

use crate::ondisk::Header;
use crate::state::RaftStateKey;
use crate::state::RaftStateValue;
use crate::state_machine::ClientLastRespValue;
use crate::state_machine::ExpireKey;
use crate::state_machine::ExpireValue;
use crate::state_machine::LogMetaKey;
use crate::state_machine::LogMetaValue;
use crate::state_machine::StateMachineMetaKey;
use crate::state_machine::StateMachineMetaValue;

/// Types for raft log in SledTree
pub struct Logs {}
impl SledKeySpace for Logs {
    const PREFIX: u8 = 1;
    const NAME: &'static str = "log";
    type K = LogIndex;
    type V = Entry;
}

/// Types for raft log meta data in SledTree
pub struct LogMeta {}
impl SledKeySpace for LogMeta {
    const PREFIX: u8 = 13;
    const NAME: &'static str = "log-meta";
    type K = LogMetaKey;
    type V = LogMetaValue;
}

/// Types for Node in SledTree
pub struct Nodes {}
impl SledKeySpace for Nodes {
    const PREFIX: u8 = 2;
    const NAME: &'static str = "node";
    type K = NodeId;
    type V = Node;
}

/// Key-Value Types for storing meta data of a raft state machine in sled::Tree, e.g. the last applied log id.
pub struct StateMachineMeta {}
impl SledKeySpace for StateMachineMeta {
    const PREFIX: u8 = 3;
    const NAME: &'static str = "sm-meta";
    type K = StateMachineMetaKey;
    type V = StateMachineMetaValue;
}

/// Key-Value Types for storing meta data of a raft in sled::Tree:
/// node_id, vote
pub struct RaftStateKV {}
impl SledKeySpace for RaftStateKV {
    const PREFIX: u8 = 4;
    const NAME: &'static str = "raft-state";
    type K = RaftStateKey;
    type V = RaftStateValue;
}

/// Stores a index for kv records with expire time.
///
/// It stores them in expire time order.
pub struct Expire {}
impl SledKeySpace for Expire {
    const PREFIX: u8 = 5;
    const NAME: &'static str = "expire";
    type K = ExpireKey;
    type V = ExpireValue;
}

/// Key-Value Types for storing general purpose kv in sled::Tree:
pub struct GenericKV {}
impl SledKeySpace for GenericKV {
    const PREFIX: u8 = 6;
    const NAME: &'static str = "generic-kv";
    type K = String;
    type V = SeqV<Vec<u8>>;
}

/// Key-Value Types for sequence number generator in sled::Tree:
pub struct Sequences {}
impl SledKeySpace for Sequences {
    const PREFIX: u8 = 7;
    const NAME: &'static str = "sequences";
    type K = String;
    type V = SeqNum;
}

pub struct ClientLastResps {}
impl SledKeySpace for ClientLastResps {
    const PREFIX: u8 = 10;
    const NAME: &'static str = "client-last-resp";
    type K = String;
    type V = ClientLastRespValue;
}

pub struct DataHeader {}
impl SledKeySpace for DataHeader {
    const PREFIX: u8 = 11;
    const NAME: &'static str = "data-header";
    type K = String;
    type V = Header;
}

/// Serialize SledKeySpace key value pair
macro_rules! serialize_for_sled {
    ($ks:tt, $key:expr, $value:expr) => {
        Ok(($ks::serialize_key($key)?, $ks::serialize_value($value)?))
    };
}

/// Convert (sub_tree_prefix, key, value, key_space1, key_space2...) into a [`RaftStoreEntry`].
///
/// It compares the sub_tree_prefix with prefix defined by every key space to determine which key space it belongs to.
macro_rules! deserialize_by_prefix {
    ($prefix: expr, $vec_key: expr, $vec_value: expr, $($key_space: tt),+ ) => {
        $(

        if <$key_space as SledKeySpace>::PREFIX == $prefix {

            let key = SledOrderedSerde::de($vec_key)?;
            let value = SledSerde::de($vec_value)?;

            // Self reference the enum that use this macro
            return Ok(Self::$key_space { key, value, });
        }
        )+
    };
}

/// Enum of key-value pairs that are used in the raft state machine.
///
/// It is a sub set of [`RaftStoreEntry`] and contains only the types used by state-machine.
#[rustfmt::skip]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SMEntry {
    DataHeader       { key: <DataHeader       as SledKeySpace>::K, value: <DataHeader       as SledKeySpace>::V, },
    Nodes            { key: <Nodes            as SledKeySpace>::K, value: <Nodes            as SledKeySpace>::V, },
    StateMachineMeta { key: <StateMachineMeta as SledKeySpace>::K, value: <StateMachineMeta as SledKeySpace>::V, },
    Expire           { key: <Expire           as SledKeySpace>::K, value: <Expire           as SledKeySpace>::V, },
    GenericKV        { key: <GenericKV        as SledKeySpace>::K, value: <GenericKV        as SledKeySpace>::V, },
    Sequences        { key: <Sequences        as SledKeySpace>::K, value: <Sequences        as SledKeySpace>::V, },
}

impl SMEntry {
    /// Serialize a key-value entry into a two elt vec of vec<u8>: `[key, value]`.
    #[rustfmt::skip]
    pub fn serialize(kv: &SMEntry) -> Result<(sled::IVec, sled::IVec), MetaStorageError> {

        match kv {
            Self::DataHeader       { key, value } => serialize_for_sled!(DataHeader,       key, value),
            Self::Nodes            { key, value } => serialize_for_sled!(Nodes,            key, value),
            Self::StateMachineMeta { key, value } => serialize_for_sled!(StateMachineMeta, key, value),
            Self::Expire           { key, value } => serialize_for_sled!(Expire,           key, value),
            Self::GenericKV        { key, value } => serialize_for_sled!(GenericKV,        key, value),
            Self::Sequences        { key, value } => serialize_for_sled!(Sequences,        key, value),
        }
    }

    /// Deserialize a serialized key-value entry `[key, value]`.
    ///
    /// It is able to deserialize openraft-v7 or openraft-v8 key-value pairs.
    /// The compatibility is provided by [`SledSerde`] implementation for value types.
    pub fn deserialize(prefix_key: &[u8], vec_value: &[u8]) -> Result<Self, MetaStorageError> {
        let prefix = prefix_key[0];
        let vec_key = &prefix_key[1..];

        deserialize_by_prefix!(
            prefix,
            vec_key,
            vec_value,
            // Available key spaces:
            DataHeader,
            Nodes,
            StateMachineMeta,
            Expire,
            GenericKV,
            Sequences
        );

        unreachable!("unknown prefix: {}", prefix);
    }

    pub fn last_applied(&self) -> Option<LogId> {
        match self {
            Self::StateMachineMeta { key, value } => {
                if *key == StateMachineMetaKey::LastApplied {
                    let last: LogId = value.clone().try_into().unwrap();
                    Some(last)
                } else {
                    None
                }
            }
            _ => None,
        }
    }
}

/// Enum of key-value pairs that are used in the raft storage impl for meta-service.
#[rustfmt::skip]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RaftStoreEntry {
    DataHeader       { key: <DataHeader       as SledKeySpace>::K, value: <DataHeader       as SledKeySpace>::V, },
    Logs             { key: <Logs             as SledKeySpace>::K, value: <Logs             as SledKeySpace>::V, },
    Nodes            { key: <Nodes            as SledKeySpace>::K, value: <Nodes            as SledKeySpace>::V, },
    StateMachineMeta { key: <StateMachineMeta as SledKeySpace>::K, value: <StateMachineMeta as SledKeySpace>::V, },
    RaftStateKV      { key: <RaftStateKV      as SledKeySpace>::K, value: <RaftStateKV      as SledKeySpace>::V, },
    Expire           { key: <Expire           as SledKeySpace>::K, value: <Expire           as SledKeySpace>::V, },
    GenericKV        { key: <GenericKV        as SledKeySpace>::K, value: <GenericKV        as SledKeySpace>::V, },
    Sequences        { key: <Sequences        as SledKeySpace>::K, value: <Sequences        as SledKeySpace>::V, },
    ClientLastResps  { key: <ClientLastResps  as SledKeySpace>::K, value: <ClientLastResps  as SledKeySpace>::V, },
    LogMeta          { key: <LogMeta          as SledKeySpace>::K, value: <LogMeta          as SledKeySpace>::V, },
}

impl RaftStoreEntry {
    /// Serialize a key-value entry into a two elt vec of vec<u8>: `[key, value]`.
    #[rustfmt::skip]
    pub fn serialize(kv: &RaftStoreEntry) -> Result<(sled::IVec, sled::IVec), MetaStorageError> {

        match kv {
            Self::DataHeader       { key, value } => serialize_for_sled!(DataHeader,       key, value),
            Self::Logs             { key, value } => serialize_for_sled!(Logs,             key, value),
            Self::Nodes            { key, value } => serialize_for_sled!(Nodes,            key, value),
            Self::StateMachineMeta { key, value } => serialize_for_sled!(StateMachineMeta, key, value),
            Self::RaftStateKV      { key, value } => serialize_for_sled!(RaftStateKV,      key, value),
            Self::Expire           { key, value } => serialize_for_sled!(Expire,           key, value),
            Self::GenericKV        { key, value } => serialize_for_sled!(GenericKV,        key, value),
            Self::Sequences        { key, value } => serialize_for_sled!(Sequences,        key, value),
            Self::ClientLastResps  { key, value } => serialize_for_sled!(ClientLastResps,  key, value),
            Self::LogMeta          { key, value } => serialize_for_sled!(LogMeta,          key, value),
        }
    }

    /// Deserialize a serialized key-value entry `[key, value]`.
    ///
    /// It is able to deserialize openraft-v7 or openraft-v8 key-value pairs.
    /// The compatibility is provided by [`SledSerde`] implementation for value types.
    pub fn deserialize(prefix_key: &[u8], vec_value: &[u8]) -> Result<Self, MetaStorageError> {
        let prefix = prefix_key[0];
        let vec_key = &prefix_key[1..];

        deserialize_by_prefix!(
            prefix,
            vec_key,
            vec_value,
            // Available key spaces:
            DataHeader,
            Logs,
            Nodes,
            StateMachineMeta,
            RaftStateKV,
            Expire,
            GenericKV,
            Sequences,
            ClientLastResps,
            LogMeta
        );

        unreachable!("unknown prefix: {}", prefix);
    }
}

impl TryInto<SMEntry> for RaftStoreEntry {
    type Error = String;

    #[rustfmt::skip]
    fn try_into(self) -> Result<SMEntry, Self::Error> {
        match self {
            Self::DataHeader       { key, value } => Ok(SMEntry::DataHeader       { key, value }),
            Self::Nodes            { key, value } => Ok(SMEntry::Nodes            { key, value }),
            Self::StateMachineMeta { key, value } => Ok(SMEntry::StateMachineMeta { key, value }),
            Self::Expire           { key, value } => Ok(SMEntry::Expire           { key, value }),
            Self::GenericKV        { key, value } => Ok(SMEntry::GenericKV        { key, value }),
            Self::Sequences        { key, value } => Ok(SMEntry::Sequences        { key, value }),

            Self::Logs             { .. } => {Err("SMEntry does not contain Logs".to_string())},
            Self::RaftStateKV      { .. } => {Err("SMEntry does not contain RaftStateKV".to_string())}
            Self::ClientLastResps  { .. } => {Err("SMEntry does not contain ClientLastResps".to_string())}
            Self::LogMeta          { .. } => {Err("SMEntry does not contain LogMeta".to_string())}
        }
    }
}
