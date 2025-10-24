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
use std::sync::Arc;
use std::time::Instant;

use databend_common_base::base::DropCallback;
use log::info;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;

/// Acquirer is used to acquire a permit for applying, without holding lock to the state machine.
pub struct WriterAcquirer {
    sem: Arc<Semaphore>,
}

impl fmt::Display for WriterAcquirer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "WriterAcquirer")
    }
}

impl WriterAcquirer {
    pub fn new(sem: Arc<Semaphore>) -> Self {
        WriterAcquirer { sem }
    }

    pub async fn acquire(self) -> WriterPermit {
        // Safe unwrap: it returns error only when semaphore is closed.
        // This semaphore does not close.
        let start = Instant::now();
        let permit = self.sem.acquire_owned().await.unwrap();
        info!("WriterPermit-Acquire: total: {:?}", start.elapsed());
        WriterPermit {
            _drop: DropCallback::new(move || {
                info!("WriterPermit-Drop: total: {:?}", start.elapsed());
            }),
            _permit: permit,
        }
    }
}

/// Exclusive writer permit for state machine operations.
///
/// This permit is backed by a semaphore with capacity 1, ensuring exclusive access.
/// When held, no other writers can modify the state machine, guaranteeing:
/// - Serialization of Raft log application
/// - Atomicity of multi-step operations (e.g., watch registration + snapshot read)
/// - Prevention of MVCC isolation issues from concurrent writes
///
/// The permit is automatically released when dropped.
pub struct WriterPermit {
    _permit: OwnedSemaphorePermit,
    _drop: DropCallback,
}
