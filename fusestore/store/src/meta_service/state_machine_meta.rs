// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt;

use async_raft::LogId;
use common_exception::ErrorCode;
use serde::Deserialize;
use serde::Serialize;
use sled::IVec;

use crate::meta_service::sled_key_space::SledKeySpace;
use crate::meta_service::SledOrderedSerde;
use crate::meta_service::SledSerde;

/// Key-Value Types for storing meta data of a raft state machine in sled::Tree, e.g. the last applied log id.
pub struct StateMachineMeta {}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum StateMachineMetaKey {
    /// The last applied log id in the state machine.
    LastApplied,
    /// Whether the state machine is initialized.
    Initialized,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum StateMachineMetaValue {
    LogId(LogId),
    Bool(bool),
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
        }
    }
}

impl SledOrderedSerde for StateMachineMetaKey {
    fn ser(&self) -> Result<IVec, ErrorCode> {
        let i = match self {
            StateMachineMetaKey::LastApplied => 1,
            StateMachineMetaKey::Initialized => 2,
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
        }

        Err(ErrorCode::MetaStoreDamaged("invalid key IVec"))
    }
}

impl SledSerde for StateMachineMetaValue {}

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

impl SledKeySpace for StateMachineMeta {
    const PREFIX: u8 = 0;
    const NAME: &'static str = "meta";
    type K = StateMachineMetaKey;
    type V = StateMachineMetaValue;
}
