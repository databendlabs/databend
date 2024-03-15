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

use log::warn;

/// To limit RPC calls done by `check_and_upgrade_to_pb()`.
/// Each RPC takes about 3ms.
/// The delay time taken by upgrading is about 300ms.
static MAX_UPGRADE: usize = 100;

/// Quota to limit number of calls for upgrading data to protobuf format.
pub struct Quota {
    state: Result<usize, ()>,

    /// The resource this quota is for.
    target: String,
}

impl Quota {
    /// Create a Quota with default limit.
    pub fn new(target: impl ToString) -> Self {
        Self::new_limit(target, MAX_UPGRADE)
    }

    /// Create a Quota with a specific limit.
    pub fn new_limit(target: impl ToString, limit: usize) -> Self {
        Quota {
            state: if limit == 0 { Err(()) } else { Ok(limit) },
            target: target.to_string(),
        }
    }

    pub fn is_used_up(&self) -> bool {
        self.state.is_err()
    }

    /// Decrement the quota by 1.
    pub fn decrement(&mut self) {
        if let Ok(n) = &mut self.state {
            *n = n.saturating_sub(1);
        }

        // Warning will be logged only once when the first time quota is used up.
        if self.state == Ok(0) {
            warn!("quota is used up for {}", self.target);
            self.state = Err(());
        }
    }
}
