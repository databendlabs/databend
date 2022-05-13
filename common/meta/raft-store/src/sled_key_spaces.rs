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
use common_meta_types::LogEntry;
use common_meta_types::LogIndex;
use common_meta_types::Node;
use common_meta_types::NodeId;
use common_meta_types::SeqNum;
use common_meta_types::SeqV;
use openraft::raft::Entry;
use serde::Deserialize;
use serde::Serialize;

use crate::state::RaftStateKey;
use crate::state::RaftStateValue;
use crate::state_machine::ClientLastRespValue;
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

pub struct ClientLastResps {}
impl SledKeySpace for ClientLastResps {
    const PREFIX: u8 = 10;
    const NAME: &'static str = "client-last-resp";
    type K = String;
    type V = ClientLastRespValue;
}

/// Key-Value types for meta service addrs in sled::Tree:
pub struct MetaSrvAddrs {}
impl SledKeySpace for MetaSrvAddrs {
    const PREFIX: u8 = 11;
    const NAME: &'static str = "metasrv-addr";
    type K = String;
    type V = String;
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
    ClientLastResps {
        key: <ClientLastResps as SledKeySpace>::K,
        value: <ClientLastResps as SledKeySpace>::V,
    },
    LogMeta {
        key: <LogMeta as SledKeySpace>::K,
        value: <LogMeta as SledKeySpace>::V,
    },
    MetaSrvAddrs {
        key: <MetaSrvAddrs as SledKeySpace>::K,
        value: <MetaSrvAddrs as SledKeySpace>::V,
    },
}
