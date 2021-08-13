// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt;

use async_raft::storage::HardState;
use async_raft::SnapshotMeta;
use common_exception::ErrorCode;
use serde::Deserialize;
use serde::Serialize;
use sled::IVec;

use crate::meta_service::NodeId;
use crate::meta_service::SledOrderedSerde;
use crate::meta_service::SledSerde;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum RaftStateKey {
    /// The node id.
    Id,

    /// Hard state of the raft log, including `current_term` and `voted_vor`.
    HardState,

    /// The id of the only active state machine.
    /// When installing a state machine snapshot:
    /// 1. A temp state machine is written into a new sled::Tree.
    /// 2. Update this field to point to the new state machine.
    /// 3. Cleanup old state machine.
    StateMachineId,

    /// The last snapshot meta data.
    /// Because membership log is not applied to state machine,
    /// Before logs are removed, the membership need to be saved.
    ///
    /// The async-raft::MemStore saves it in a removed log slot,
    /// which may mess up the replication:
    /// Replication has chance sending this pointer log that was an client data.
    ///
    /// The presence of snapshot meta does not mean there is a valid snapshot data.
    /// Since snapshot data is not saved on disk.
    /// A snapshot meta guarantees no smaller snapshot will be installed when installing snapshot,
    /// and no membership config is lost when removing logs that are included in snapshot.
    SnapshotMeta,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RaftStateValue {
    NodeId(NodeId),
    HardState(HardState),
    /// active state machine, previous state machine
    StateMachineId((u64, u64)),
    SnapshotMeta(SnapshotMeta),
}

impl fmt::Display for RaftStateKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RaftStateKey::Id => {
                write!(f, "Id")
            }
            RaftStateKey::HardState => {
                write!(f, "HardState")
            }
            RaftStateKey::StateMachineId => {
                write!(f, "StateMachineId")
            }
            RaftStateKey::SnapshotMeta => {
                write!(f, "SnapshotMeta")
            }
        }
    }
}

impl SledOrderedSerde for RaftStateKey {
    fn ser(&self) -> Result<IVec, ErrorCode> {
        let i = match self {
            RaftStateKey::Id => 1,
            RaftStateKey::HardState => 2,
            RaftStateKey::StateMachineId => 3,
            RaftStateKey::SnapshotMeta => 4,
        };

        Ok(IVec::from(&[i]))
    }

    fn de<V: AsRef<[u8]>>(v: V) -> Result<Self, ErrorCode>
    where Self: Sized {
        let slice = v.as_ref();
        if slice[0] == 1 {
            return Ok(RaftStateKey::Id);
        } else if slice[0] == 2 {
            return Ok(RaftStateKey::HardState);
        } else if slice[0] == 3 {
            return Ok(RaftStateKey::StateMachineId);
        } else if slice[0] == 4 {
            return Ok(RaftStateKey::SnapshotMeta);
        }

        Err(ErrorCode::MetaStoreDamaged("invalid key IVec"))
    }
}

impl SledSerde for RaftStateValue {}

impl From<RaftStateValue> for NodeId {
    fn from(v: RaftStateValue) -> Self {
        match v {
            RaftStateValue::NodeId(x) => x,
            _ => panic!("expect NodeId"),
        }
    }
}

impl From<RaftStateValue> for HardState {
    fn from(v: RaftStateValue) -> Self {
        match v {
            RaftStateValue::HardState(x) => x,
            _ => panic!("expect HardState"),
        }
    }
}

impl From<RaftStateValue> for (u64, u64) {
    fn from(v: RaftStateValue) -> Self {
        match v {
            RaftStateValue::StateMachineId(x) => x,
            _ => panic!("expect StateMachineId"),
        }
    }
}

impl From<RaftStateValue> for SnapshotMeta {
    fn from(v: RaftStateValue) -> Self {
        match v {
            RaftStateValue::SnapshotMeta(x) => x,
            _ => panic!("expect Membership"),
        }
    }
}
