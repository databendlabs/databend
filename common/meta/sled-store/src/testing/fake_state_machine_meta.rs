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
use std::fmt;

use async_raft::raft::MembershipConfig;
use async_raft::LogId;
use common_exception::ErrorCode;
use serde::Deserialize;
use serde::Serialize;
use sled::IVec;

use crate::SledOrderedSerde;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum StateMachineMetaKey {
    /// The last applied log id in the state machine.
    LastApplied,

    /// Whether the state machine is initialized.
    Initialized,

    /// The last membership config
    LastMembership,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum StateMachineMetaValue {
    LogId(LogId),
    Bool(bool),
    Membership(MembershipConfig),
}

impl fmt::Display for StateMachineMetaKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StateMachineMetaKey::LastApplied => {
                write!(f, "last-applied")
            }
            StateMachineMetaKey::Initialized => {
                write!(f, "initialized")
            }
            StateMachineMetaKey::LastMembership => {
                write!(f, "last-membership")
            }
        }
    }
}

impl SledOrderedSerde for StateMachineMetaKey {
    fn ser(&self) -> Result<IVec, ErrorCode> {
        let i = match self {
            StateMachineMetaKey::LastApplied => 1,
            StateMachineMetaKey::Initialized => 2,
            StateMachineMetaKey::LastMembership => 3,
        };

        Ok(IVec::from(&[i]))
    }

    fn de<V: AsRef<[u8]>>(v: V) -> Result<Self, ErrorCode>
    where Self: Sized {
        let slice = v.as_ref();
        if slice[0] == 1 {
            return Ok(StateMachineMetaKey::LastApplied);
        } else if slice[0] == 2 {
            return Ok(StateMachineMetaKey::Initialized);
        } else if slice[0] == 3 {
            return Ok(StateMachineMetaKey::LastMembership);
        }

        Err(ErrorCode::MetaStoreDamaged("invalid key IVec"))
    }
}

impl From<StateMachineMetaValue> for LogId {
    fn from(v: StateMachineMetaValue) -> Self {
        match v {
            StateMachineMetaValue::LogId(x) => x,
            _ => panic!("expect LogId"),
        }
    }
}

impl From<StateMachineMetaValue> for bool {
    fn from(v: StateMachineMetaValue) -> Self {
        match v {
            StateMachineMetaValue::Bool(x) => x,
            _ => panic!("expect LogId"),
        }
    }
}
impl From<StateMachineMetaValue> for MembershipConfig {
    fn from(v: StateMachineMetaValue) -> Self {
        match v {
            StateMachineMetaValue::Membership(x) => x,
            _ => panic!("expect Membership"),
        }
    }
}
