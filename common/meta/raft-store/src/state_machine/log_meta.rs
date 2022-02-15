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

use common_meta_sled_store::openraft;
use common_meta_sled_store::sled;
use common_meta_sled_store::SledOrderedSerde;
use common_meta_types::anyerror::AnyError;
use common_meta_types::MetaStorageError;
use openraft::LogId;
use serde::Deserialize;
use serde::Serialize;
use sled::IVec;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum LogMetaKey {
    /// The last purged log id in the log.
    ///
    /// Log entries are purged after being applied to state machine.
    /// Even when all logs are purged the last purged log id has to be stored.
    /// Because raft replication requires logs to be consecutive.
    LastPurged,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, derive_more::TryInto)]
pub enum LogMetaValue {
    LogId(LogId),
}

impl fmt::Display for LogMetaKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LogMetaKey::LastPurged => {
                write!(f, "last-purged")
            }
        }
    }
}

impl SledOrderedSerde for LogMetaKey {
    fn ser(&self) -> Result<IVec, MetaStorageError> {
        let i = match self {
            LogMetaKey::LastPurged => 1,
        };

        Ok(IVec::from(&[i]))
    }

    fn de<V: AsRef<[u8]>>(v: V) -> Result<Self, MetaStorageError>
    where Self: Sized {
        let slice = v.as_ref();
        if slice[0] == 1 {
            return Ok(LogMetaKey::LastPurged);
        }

        Err(MetaStorageError::SledError(AnyError::error(
            "invalid key IVec",
        )))
    }
}
