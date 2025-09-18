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

/// Acquirer is used to acquire a permit for compaction, without holding lock to the state machine.
pub struct CompactorAcquirer {
    name: String,
    sem: Arc<Semaphore>,
}

impl fmt::Display for CompactorAcquirer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CompactorAcquirer({})", self.name)
    }
}

impl CompactorAcquirer {
    pub fn new(sem: Arc<Semaphore>, name: impl ToString) -> Self {
        CompactorAcquirer {
            name: name.to_string(),
            sem,
        }
    }

    pub async fn acquire(self) -> CompactorPermit {
        let start = Instant::now();

        // Safe unwrap: it returns error only when semaphore is closed.
        // This semaphore does not close.
        let permit = self.sem.acquire_owned().await.unwrap();

        let name = self.name.clone();

        info!(
            "CompactorPermit({})-Acquire: total: {:?}",
            name,
            start.elapsed()
        );

        CompactorPermit {
            _permit: permit,
            _drop: DropCallback::new(move || {
                info!(
                    "CompactorPermit({})-Drop: total: {:?}",
                    name,
                    start.elapsed()
                );
            }),
        }
    }
}

#[derive(Debug)]
pub struct CompactorPermit {
    _permit: OwnedSemaphorePermit,
    _drop: DropCallback,
}
