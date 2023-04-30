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

use common_meta_sled_store::sled;
use common_meta_sled_store::SledBytesError;
use common_meta_sled_store::SledOrderedSerde;
use common_meta_types::anyerror::AnyError;
use common_meta_types::NodeId;
use common_meta_types::Vote;
use serde::Deserialize;
use serde::Serialize;
use sled::IVec;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RaftStateValue {
    NodeId(NodeId),

    HardState(Vote),

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
    fn ser(&self) -> Result<IVec, SledBytesError> {
        let i = match self {
            RaftStateKey::Id => 1,
            RaftStateKey::HardState => 2,
            RaftStateKey::StateMachineId => 3,
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

pub(crate) mod compat_with_07 {
    use common_meta_sled_store::SledBytesError;
    use common_meta_sled_store::SledSerde;
    use common_meta_types::compat07;
    use common_meta_types::NodeId;
    use openraft::compat::Upgrade;

    use crate::state::RaftStateValue;

    #[derive(Debug, serde::Serialize, serde::Deserialize)]
    pub enum RaftStateValueCompat {
        NodeId(NodeId),
        HardState(compat07::Vote),
        StateMachineId((u64, u64)),
    }

    impl Upgrade<RaftStateValue> for RaftStateValueCompat {
        #[rustfmt::skip]
        fn upgrade(self) -> RaftStateValue {
            match self{
                Self::NodeId(nid)       => RaftStateValue::NodeId(nid),
                Self::HardState(v)      => RaftStateValue::HardState(v.upgrade()),
                Self::StateMachineId(x) => RaftStateValue::StateMachineId(x),
            }
        }
    }

    impl SledSerde for RaftStateValue {
        fn de<T: AsRef<[u8]>>(v: T) -> Result<Self, SledBytesError>
        where Self: Sized {
            let s: RaftStateValueCompat = serde_json::from_slice(v.as_ref())?;

            let v = match s {
                RaftStateValueCompat::NodeId(nid) => Self::NodeId(nid),
                RaftStateValueCompat::HardState(h) => Self::HardState(h.upgrade()),
                RaftStateValueCompat::StateMachineId(x) => Self::StateMachineId(x),
            };
            Ok(v)
        }
    }
}
