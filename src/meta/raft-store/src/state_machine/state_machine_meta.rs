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
use databend_common_meta_types::StoredMembership;
use serde::Deserialize;
use serde::Serialize;
use sled::IVec;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum StateMachineMetaKey {
    /// The last applied log id in the state machine.
    LastApplied,

    /// Whether the state machine is initialized.
    ///
    /// No longer used since `DataVersion::V0002`
    Initialized,

    /// The last membership config
    LastMembership,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, derive_more::TryInto)]
pub enum StateMachineMetaValue {
    LogId(LogId),
    Bool(bool),
    Membership(StoredMembership),
}

impl fmt::Display for StateMachineMetaKey {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
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
    fn ser(&self) -> Result<IVec, SledBytesError> {
        let i = match self {
            StateMachineMetaKey::LastApplied => 1,
            StateMachineMetaKey::Initialized => 2,
            StateMachineMetaKey::LastMembership => 3,
        };

        Ok(IVec::from(&[i]))
    }

    fn de<V: AsRef<[u8]>>(v: V) -> Result<Self, SledBytesError>
    where Self: Sized {
        let slice = v.as_ref();
        if slice[0] == 1 {
            return Ok(StateMachineMetaKey::LastApplied);
        } else if slice[0] == 2 {
            return Ok(StateMachineMetaKey::Initialized);
        } else if slice[0] == 3 {
            return Ok(StateMachineMetaKey::LastMembership);
        }

        Err(SledBytesError::new(&AnyError::error("invalid key IVec")))
    }
}

impl SledSerde for StateMachineMetaValue {
    fn de<T: AsRef<[u8]>>(v: T) -> Result<Self, SledBytesError>
    where Self: Sized {
        let s: StateMachineMetaValue = serde_json::from_slice(v.as_ref())?;
        Ok(s)
    }
}
