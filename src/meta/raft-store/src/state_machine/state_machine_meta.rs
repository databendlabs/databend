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
use common_meta_types::LogId;
use common_meta_types::StoredMembership;
use serde::Deserialize;
use serde::Serialize;
use sled::IVec;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum StateMachineMetaKey {
    /// The last applied log id in the state machine.
    LastApplied,

    /// Whether the state machine is initialized.
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

pub(crate) mod compat_with_07 {
    use common_meta_sled_store::SledBytesError;
    use common_meta_sled_store::SledSerde;
    use common_meta_types::compat07;
    use openraft::compat::Upgrade;

    use crate::state_machine::StateMachineMetaValue;

    #[derive(Debug, serde::Serialize, serde::Deserialize)]
    pub enum StateMachineMetaValueCompat {
        LogId(compat07::LogId),
        Bool(bool),
        Membership(compat07::StoredMembership),
    }

    impl Upgrade<StateMachineMetaValue> for StateMachineMetaValueCompat {
        #[rustfmt::skip]
        fn upgrade(self) -> StateMachineMetaValue {
            match self {
                Self::LogId(lid)    => StateMachineMetaValue::LogId(lid.upgrade()),
                Self::Bool(b)       => StateMachineMetaValue::Bool(b),
                Self::Membership(m) => StateMachineMetaValue::Membership(m.upgrade()),
            }
        }
    }

    impl SledSerde for StateMachineMetaValue {
        fn de<T: AsRef<[u8]>>(v: T) -> Result<Self, SledBytesError>
        where Self: Sized {
            let s: StateMachineMetaValueCompat = serde_json::from_slice(v.as_ref())?;

            let v = match s {
                StateMachineMetaValueCompat::LogId(lid) => Self::LogId(lid.upgrade()),
                StateMachineMetaValueCompat::Bool(b) => Self::Bool(b),
                StateMachineMetaValueCompat::Membership(m) => Self::Membership(m.upgrade()),
            };

            Ok(v)
        }
    }
}
