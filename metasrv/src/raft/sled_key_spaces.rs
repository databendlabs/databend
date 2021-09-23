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

use async_raft::raft::Entry;
use common_metatypes::KVValue;
use common_metatypes::LogEntry;
use common_metatypes::LogIndex;
use common_metatypes::Node;
use common_metatypes::NodeId;
use common_metatypes::SeqValue;
use common_sled_store::SeqNum;
use common_sled_store::SledKeySpace;

use crate::raft::state::RaftStateKey;
use crate::raft::state::RaftStateValue;
use crate::raft::state_machine::StateMachineMetaKey;
use crate::raft::state_machine::StateMachineMetaValue;

/// Types for raft log in SledTree
pub struct Logs {}
impl SledKeySpace for Logs {
    const PREFIX: u8 = 1;
    const NAME: &'static str = "log";
    type K = LogIndex;
    type V = Entry<LogEntry>;
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

/// Key-Value Types for storing DFS files in sled::Tree:
pub struct Files {}
impl SledKeySpace for Files {
    const PREFIX: u8 = 5;
    const NAME: &'static str = "files";
    type K = String;
    type V = String;
}

/// Key-Value Types for storing general purpose kv in sled::Tree:
pub struct GenericKV {}
impl SledKeySpace for GenericKV {
    const PREFIX: u8 = 6;
    const NAME: &'static str = "generic-kv";
    type K = String;
    type V = SeqValue<KVValue<Vec<u8>>>;
}

/// Key-Value Types for sequence number generator in sled::Tree:
pub struct Sequences {}
impl SledKeySpace for Sequences {
    const PREFIX: u8 = 7;
    const NAME: &'static str = "sequences";
    type K = String;
    type V = SeqNum;
}
