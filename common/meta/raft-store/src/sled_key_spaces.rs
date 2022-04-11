// Copyright 2021 Datafuse Labs.
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

use common_meta_sled_store::openraft;
use common_meta_sled_store::SledKeySpace;
use common_meta_types::DatabaseMeta;
use common_meta_types::LogEntry;
use common_meta_types::LogIndex;
use common_meta_types::Node;
use common_meta_types::NodeId;
use common_meta_types::SeqNum;
use common_meta_types::SeqV;
use common_meta_types::ShareInfo;
use common_meta_types::TableMeta;
use openraft::raft::Entry;
use serde::Deserialize;
use serde::Serialize;

use crate::state::RaftStateKey;
use crate::state::RaftStateValue;
use crate::state_machine::share_inbound::ShareInboundKey;
use crate::state_machine::share_inbound::ShareInboundValue;
use crate::state_machine::share_lookup::ShareLookupKey;
use crate::state_machine::share_lookup::ShareLookupValue;
use crate::state_machine::share_outbound::ShareOutboundKey;
use crate::state_machine::share_outbound::ShareOutboundValue;
use crate::state_machine::table_lookup::TableLookupValue;
use crate::state_machine::ClientLastRespValue;
use crate::state_machine::DatabaseLookupKey;
use crate::state_machine::LogMetaKey;
use crate::state_machine::LogMetaValue;
use crate::state_machine::StateMachineMetaKey;
use crate::state_machine::StateMachineMetaValue;
use crate::state_machine::TableLookupKey;

/// Types for raft log in SledTree
pub struct Logs {}
impl SledKeySpace for Logs {
    const PREFIX: u8 = 1;
    const NAME: &'static str = "log";
    type K = LogIndex;
    type V = Entry<LogEntry>;
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
/// id: NodeId,
/// hard_state:
///      current_term,
///      voted_for,
pub struct RaftStateKV {}
impl SledKeySpace for RaftStateKV {
    const PREFIX: u8 = 4;
    const NAME: &'static str = "raft-state";
    type K = RaftStateKey;
    type V = RaftStateValue;
}

// preserved PREFIX = 5

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

/// Key-Value Types for storing general purpose kv in sled::Tree:
pub struct Databases {}
impl SledKeySpace for Databases {
    const PREFIX: u8 = 8;
    const NAME: &'static str = "databases";
    type K = u64;
    type V = SeqV<DatabaseMeta>;
}

pub struct DatabaseLookup {}
impl SledKeySpace for DatabaseLookup {
    const PREFIX: u8 = 12;
    const NAME: &'static str = "database-lookup";
    type K = DatabaseLookupKey;
    type V = SeqV<u64>;
}

pub struct Tables {}
impl SledKeySpace for Tables {
    const PREFIX: u8 = 9;
    const NAME: &'static str = "tables";
    type K = u64;
    type V = SeqV<TableMeta>;
}

pub struct ClientLastResps {}
impl SledKeySpace for ClientLastResps {
    const PREFIX: u8 = 10;
    const NAME: &'static str = "client-last-resp";
    type K = String;
    type V = ClientLastRespValue;
}

pub struct TableLookup {}
impl SledKeySpace for TableLookup {
    const PREFIX: u8 = 11;
    const NAME: &'static str = "table-lookup";
    type K = TableLookupKey;
    type V = SeqV<TableLookupValue>;
}

pub struct Shares {}
impl SledKeySpace for Shares {
    const PREFIX: u8 = 12;
    const NAME: &'static str = "shares";
    type K = u64;
    type V = SeqV<ShareInfo>;
}

pub struct ShareLookup {}
impl SledKeySpace for ShareLookup {
    const PREFIX: u8 = 14;
    const NAME: &'static str = "share-look-up";
    type K = ShareLookupKey;
    type V = SeqV<ShareLookupValue>;
}

pub struct ShareOutbounds {}
impl SledKeySpace for ShareOutbounds {
    const PREFIX: u8 = 15;
    const NAME: &'static str = "share-outbounds";
    type K = ShareOutboundKey;
    type V = SeqV<ShareOutboundValue>;
}

pub struct ShareInbounds {}
impl SledKeySpace for ShareInbounds {
    const PREFIX: u8 = 16;
    const NAME: &'static str = "share-inbounds";
    type K = ShareInboundKey;
    type V = SeqV<ShareInboundValue>;
}

/// Enum of key-value pair types of all key spaces.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum KeySpaceKV {
    Logs {
        key: <Logs as SledKeySpace>::K,
        value: <Logs as SledKeySpace>::V,
    },
    Nodes {
        key: <Nodes as SledKeySpace>::K,
        value: <Nodes as SledKeySpace>::V,
    },
    StateMachineMeta {
        key: <StateMachineMeta as SledKeySpace>::K,
        value: <StateMachineMeta as SledKeySpace>::V,
    },
    RaftStateKV {
        key: <RaftStateKV as SledKeySpace>::K,
        value: <RaftStateKV as SledKeySpace>::V,
    },
    GenericKV {
        key: <GenericKV as SledKeySpace>::K,
        value: <GenericKV as SledKeySpace>::V,
    },
    Sequences {
        key: <Sequences as SledKeySpace>::K,
        value: <Sequences as SledKeySpace>::V,
    },
    Databases {
        key: <Databases as SledKeySpace>::K,
        value: <Databases as SledKeySpace>::V,
    },
    Tables {
        key: <Tables as SledKeySpace>::K,
        value: <Tables as SledKeySpace>::V,
    },
    ClientLastResps {
        key: <ClientLastResps as SledKeySpace>::K,
        value: <ClientLastResps as SledKeySpace>::V,
    },
    TableLookup {
        key: <TableLookup as SledKeySpace>::K,
        value: <TableLookup as SledKeySpace>::V,
    },
    DatabaseLookup {
        key: <DatabaseLookup as SledKeySpace>::K,
        value: <DatabaseLookup as SledKeySpace>::V,
    },
    LogMeta {
        key: <LogMeta as SledKeySpace>::K,
        value: <LogMeta as SledKeySpace>::V,
    },
    Share {
        key: <Shares as SledKeySpace>::K,
        value: <Shares as SledKeySpace>::V,
    },
    ShareLookup {
        key: <ShareLookup as SledKeySpace>::K,
        value: <ShareLookup as SledKeySpace>::V,
    },
    ShareOutbound {
        key: <ShareOutbounds as SledKeySpace>::K,
        value: <ShareOutbounds as SledKeySpace>::V,
    },
    ShareInbound {
        key: <ShareInbounds as SledKeySpace>::K,
        value: <ShareInbounds as SledKeySpace>::V,
    },
}
