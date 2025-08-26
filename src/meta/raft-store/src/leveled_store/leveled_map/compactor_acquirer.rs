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

use std::sync::Arc;

use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;

/// Acquirer is used to acquire a permit for compaction, without holding lock to the state machine.
pub struct CompactorAcquirer {
    sem: Arc<Semaphore>,
}

impl CompactorAcquirer {
    pub fn new(sem: Arc<Semaphore>) -> Self {
        CompactorAcquirer { sem }
    }

    pub async fn acquire(self) -> CompactorPermit {
        // Safe unwrap: it returns error only when semaphore is closed.
        // This semaphore does not close.
        let permit = self.sem.acquire_owned().await.unwrap();
        CompactorPermit { _permit: permit }
    }
}

#[derive(Debug)]
pub struct CompactorPermit {
    _permit: OwnedSemaphorePermit,
}
