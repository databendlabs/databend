// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt;

use async_raft::storage::HardState;
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
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum RaftStateValue {
    NodeId(NodeId),
    HardState(HardState),
    /// active state machine, previous state machine
    StateMachineId((u64, u64)),
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
        }
    }
}

impl SledOrderedSerde for RaftStateKey {
    fn ser(&self) -> Result<IVec, ErrorCode> {
        let i = match self {
            RaftStateKey::Id => 1,
            RaftStateKey::HardState => 2,
            RaftStateKey::StateMachineId => 3,
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
