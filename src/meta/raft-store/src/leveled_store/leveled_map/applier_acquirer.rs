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
use std::time::Instant;

use log::info;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;

/// Acquirer is used to acquire a permit for applying, without holding lock to the state machine.
pub struct WriterAcquirer {
    sem: Arc<Semaphore>,
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
            start: Instant::now(),
            _permit: permit,
        }
    }
}

/// ApplierPermit is used to acquire a permit for applying changes to the state machine.
pub struct WriterPermit {
    start: Instant,
    _permit: OwnedSemaphorePermit,
}

impl Drop for WriterPermit {
    fn drop(&mut self) {
        info!("WriterPermit-Drop: total: {:?}", self.start.elapsed());
    }
}
