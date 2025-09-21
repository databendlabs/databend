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

//! Storage partitioning by data type with unique prefixes.
//!
//! Each key space stores a specific data type:
//! - **Logs** (V003): Raft log entries by index
//! - **Nodes**: Cluster membership by node ID
//! - **StateMachineMeta**: Last applied log and metadata
//! - **RaftStateKV** (V003): Node state (ID, vote, committed log)
//! - **Expire**: TTL expiration index by time
//! - **GenericKV**: User data with sequence numbers
//! - **Sequences**: Monotonic sequence generators
//! - **LogMeta** (V003): Log metadata for purging
//!
//! V003 uses sled storage (legacy), V004 uses separate raft log with leveled state machine.

use databend_common_meta_sled_store::sled;
use databend_common_meta_sled_store::SledKeySpace;
use databend_common_meta_sled_store::SledOrderedSerde;
use databend_common_meta_sled_store::SledSerde;
use databend_common_meta_stoerr::MetaStorageError;
use databend_common_meta_types::node::Node;
use databend_common_meta_types::raft_types::Entry;
use databend_common_meta_types::raft_types::LogId;
use databend_common_meta_types::raft_types::LogIndex;
use databend_common_meta_types::raft_types::NodeId;
use databend_common_meta_types::raft_types::Vote;
use databend_common_meta_types::SeqNum;
use databend_common_meta_types::SeqV;
use serde::Deserialize;
use serde::Serialize;
use state_machine_api::ExpireKey;
use state_machine_api::ExpireValue;

use crate::ondisk::Header;
use crate::ondisk::OnDisk;
use crate::state::RaftStateKey;
use crate::state::RaftStateValue;
use crate::state_machine::LogMetaKey;
use crate::state_machine::LogMetaValue;
use crate::state_machine::StateMachineMetaKey;
use crate::state_machine::StateMachineMetaValue;

/// Raft log entries storage key space (V003 only).
///
/// Maps log index to raft entries. In V004, logs are stored separately
/// in WAL format for better performance.
/// - Key: [`LogIndex`] (u64) - Sequential log entry index
/// - Value: [`Entry`] - Complete raft log entry with payload
pub struct Logs {}
impl SledKeySpace for Logs {
    const PREFIX: u8 = 1;
    const NAME: &'static str = "log";
    type K = LogIndex;
    type V = Entry;
}

/// Log metadata storage key space (V003 only).
///
/// Stores metadata about raft logs for purging and management.
/// Used to track log ranges and cleanup operations.
/// - Key: [`LogMetaKey`] - Metadata type identifier
/// - Value: [`LogMetaValue`] - Log metadata information
pub struct LogMeta {}
impl SledKeySpace for LogMeta {
    const PREFIX: u8 = 13;
    const NAME: &'static str = "log-meta";
    type K = LogMetaKey;
    type V = LogMetaValue;
}

/// Cluster node information storage key space.
///
/// Maps node IDs to node configuration and endpoint information.
/// Used for cluster membership management and node discovery.
/// - Key: [`NodeId`] (u64) - Unique node identifier
/// - Value: [`Node`] - Node configuration with endpoint and metadata
pub struct Nodes {}
impl SledKeySpace for Nodes {
    const PREFIX: u8 = 2;
    const NAME: &'static str = "node";
    type K = NodeId;
    type V = Node;
}

/// State machine metadata storage key space.
///
/// Stores critical state machine metadata like last applied log ID
/// and other operational state required for consistency.
/// - Key: [`StateMachineMetaKey`] - Metadata type identifier
/// - Value: [`StateMachineMetaValue`] - State machine metadata
pub struct StateMachineMeta {}
impl SledKeySpace for StateMachineMeta {
    const PREFIX: u8 = 3;
    const NAME: &'static str = "sm-meta";
    type K = StateMachineMetaKey;
    type V = StateMachineMetaValue;
}

/// Raft consensus state storage key space (V003 only).
///
/// Stores core raft state including node ID, vote information,
/// and committed log index. Critical for raft consensus protocol.
/// - Key: [`RaftStateKey`] - State type identifier
/// - Value: [`RaftStateValue`] - Raft state data
pub struct RaftStateKV {}
impl SledKeySpace for RaftStateKV {
    const PREFIX: u8 = 4;
    const NAME: &'static str = "raft-state";
    type K = RaftStateKey;
    type V = RaftStateValue;
}

/// TTL expiration index storage key space.
///
/// Secondary index for records with expiration times, ordered by expire time.
/// Enables efficient cleanup of expired entries during log application.
/// - Key: [`ExpireKey`] - Expiration time and sequence
/// - Value: [`ExpireValue`] - Original key that expires
pub struct Expire {}
impl SledKeySpace for Expire {
    const PREFIX: u8 = 5;
    const NAME: &'static str = "expire";
    type K = ExpireKey;
    type V = ExpireValue;
}

/// User data storage key space.
///
/// Primary storage for application key-value data with sequence numbers
/// for versioning and consistency. Supports TTL and conditional operations.
/// - Key: [`String`] - User-defined key
/// - Value: [`SeqV<Vec<u8>>`] - Sequenced value with metadata
pub struct GenericKV {}
impl SledKeySpace for GenericKV {
    const PREFIX: u8 = 6;
    const NAME: &'static str = "generic-kv";
    type K = String;
    type V = SeqV<Vec<u8>>;
}

/// Monotonic sequence generator storage key space.
///
/// Provides atomic sequence number generation for various purposes
/// like auto-incrementing IDs and sequential key generation.
/// - Key: [`String`] - Sequence name identifier
/// - Value: [`SeqNum`] - Current sequence number
pub struct Sequences {}
impl SledKeySpace for Sequences {
    const PREFIX: u8 = 7;
    const NAME: &'static str = "sequences";
    type K = String;
    type V = SeqNum;
}

// Reserved: removed: `pub struct ClientLastResps {}`, PREFIX = 10;

/// Storage metadata header key space.
///
/// Stores version information and metadata about the storage format
/// for compatibility checking and data migration purposes.
/// - Key: [`String`] - Header type identifier
/// - Value: [`Header`] - Storage format metadata
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
    // V003 only
    Logs             { key: <Logs             as SledKeySpace>::K, value: <Logs             as SledKeySpace>::V, },
    Nodes            { key: <Nodes            as SledKeySpace>::K, value: <Nodes            as SledKeySpace>::V, },
    StateMachineMeta { key: <StateMachineMeta as SledKeySpace>::K, value: <StateMachineMeta as SledKeySpace>::V, },
    // V003 only
    RaftStateKV      { key: <RaftStateKV      as SledKeySpace>::K, value: <RaftStateKV      as SledKeySpace>::V, },
    Expire           { key: <Expire           as SledKeySpace>::K, value: <Expire           as SledKeySpace>::V, },
    GenericKV        { key: <GenericKV        as SledKeySpace>::K, value: <GenericKV        as SledKeySpace>::V, },
    Sequences        { key: <Sequences        as SledKeySpace>::K, value: <Sequences        as SledKeySpace>::V, },
    // V003 only
    LogMeta          { key: <LogMeta          as SledKeySpace>::K, value: <LogMeta          as SledKeySpace>::V, },

    // V004 log:
    LogEntry(Entry),
    NodeId(Option<NodeId>),
    Vote(Option<Vote>),
    Committed(Option<LogId>),
    Purged(Option<LogId>),
}

impl RaftStoreEntry {
    /// Upgrade V003 to V004
    pub fn upgrade(self) -> Self {
        match self.clone() {
            Self::Logs { key: _, value } => Self::LogEntry(value),
            Self::RaftStateKV { key: _, value } => match value {
                RaftStateValue::NodeId(node_id) => Self::NodeId(Some(node_id)),
                RaftStateValue::HardState(vote) => Self::Vote(Some(vote)),
                RaftStateValue::Committed(committed) => Self::Committed(committed),
                RaftStateValue::StateMachineId(_) => self,
            },
            Self::LogMeta { key: _, value } => Self::Purged(Some(value.log_id())),

            RaftStoreEntry::DataHeader { .. }
            | RaftStoreEntry::Nodes { .. }
            | RaftStoreEntry::StateMachineMeta { .. }
            | RaftStoreEntry::Expire { .. }
            | RaftStoreEntry::GenericKV { .. }
            | RaftStoreEntry::Sequences { .. }
            | RaftStoreEntry::LogEntry(_)
            | RaftStoreEntry::NodeId(_)
            | RaftStoreEntry::Vote(_)
            | RaftStoreEntry::Committed(_)
            | RaftStoreEntry::Purged(_) => self,
        }
    }

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
            Self::LogMeta          { key, value } => serialize_for_sled!(LogMeta,          key, value),

            RaftStoreEntry::LogEntry(_) |
            RaftStoreEntry::NodeId(_)|
            RaftStoreEntry::Vote(_) |
            RaftStoreEntry::Committed(_)|
            RaftStoreEntry::Purged(_) => {
                unreachable!("V004 entries should not be serialized for sled")
            }
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
            LogMeta
        );

        unreachable!("unknown prefix: {}", prefix);
    }

    pub fn new_header(header: Header) -> Self {
        RaftStoreEntry::DataHeader {
            key: OnDisk::KEY_HEADER.to_string(),
            value: header,
        }
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
            Self::LogMeta          { .. } => {Err("SMEntry does not contain LogMeta".to_string())}


            Self::LogEntry        (_) => {Err("SMEntry does not contain LogEntry".to_string())}
            Self::Vote            (_) => {Err("SMEntry does not contain Vote".to_string())}
            Self::NodeId          (_) => {Err("SMEntry does not contain NodeId".to_string())}
            Self::Committed       (_) => {Err("SMEntry does not contain Committed".to_string())}
            Self::Purged          (_) => {Err("SMEntry does not contain Purged".to_string())}

        }
    }
}
