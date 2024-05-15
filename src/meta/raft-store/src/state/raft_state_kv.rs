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

use databend_common_meta_sled_store::sled;
use databend_common_meta_sled_store::SledBytesError;
use databend_common_meta_sled_store::SledOrderedSerde;
use databend_common_meta_sled_store::SledSerde;
use databend_common_meta_types::anyerror::AnyError;
use databend_common_meta_types::LogId;
use databend_common_meta_types::NodeId;
use databend_common_meta_types::Vote;
use serde::Deserialize;
use serde::Serialize;
use sled::IVec;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum RaftStateKey {
    /// The node id.
    Id,

    /// Hard state of the raft log, including `current_term` and `voted_vor`.
    HardState,

    // TODO: remove this field. It is not used any more. It is kept only for compatibility.
    /// The id of the only active state machine.
    /// When installing a state machine snapshot:
    /// 1. A temp state machine is written into a new sled::Tree.
    /// 2. Update this field to point to the new state machine.
    /// 3. Cleanup old state machine.
    StateMachineId,

    /// The last committed log id
    Committed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RaftStateValue {
    NodeId(NodeId),

    HardState(Vote),

    /// active state machine, previous state machine
    StateMachineId((u64, u64)),

    Committed(Option<LogId>),
}

impl fmt::Display for RaftStateKey {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl SledOrderedSerde for RaftStateKey {
    fn ser(&self) -> Result<IVec, SledBytesError> {
        let i = match self {
            RaftStateKey::Id => 1,
            RaftStateKey::HardState => 2,
            RaftStateKey::StateMachineId => 3,
            RaftStateKey::Committed => 4,
        };

        Ok(IVec::from(&[i]))
    }

    fn de<V: AsRef<[u8]>>(v: V) -> Result<Self, SledBytesError>
    where Self: Sized {
        let slice = v.as_ref();
        if slice[0] == 1 {
            return Ok(RaftStateKey::Id);
        } else if slice[0] == 2 {
            return Ok(RaftStateKey::HardState);
        } else if slice[0] == 3 {
            return Ok(RaftStateKey::StateMachineId);
        } else if slice[0] == 4 {
            return Ok(RaftStateKey::Committed);
        }

        Err(SledBytesError::new(&AnyError::error("invalid key IVec")))
    }
}

impl From<RaftStateValue> for NodeId {
    fn from(v: RaftStateValue) -> Self {
        match v {
            RaftStateValue::NodeId(x) => x,
            _ => panic!("expect NodeId"),
        }
    }
}

impl From<RaftStateValue> for Vote {
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

impl From<RaftStateValue> for Option<LogId> {
    fn from(v: RaftStateValue) -> Self {
        match v {
            RaftStateValue::Committed(x) => x,
            _ => panic!("expect Committed"),
        }
    }
}

impl SledSerde for RaftStateValue {
    fn de<T: AsRef<[u8]>>(v: T) -> Result<Self, SledBytesError>
    where Self: Sized {
        let s = serde_json::from_slice(v.as_ref())?;
        Ok(s)
    }
}
