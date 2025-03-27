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

use crate::storage::PermitEntry;
use crate::storage::PermitKey;

/// The [`PermitEntry`] has been removed from the meta-service before being acquired.
///
/// Usually this happens when the semaphore ttl is expired.
#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
#[error("EarlyRemoved: Semaphore PermitEntry is removed before being acquired: key:{permit_key} entry:{permit_entry}")]
pub struct EarlyRemoved {
    permit_key: PermitKey,
    permit_entry: PermitEntry,
}

impl EarlyRemoved {
    pub fn new(permit_key: PermitKey, permit_entry: PermitEntry) -> Self {
        Self {
            permit_key,
            permit_entry,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Test the display of EarlyRemoved
    #[test]
    fn test_display() {
        let err = EarlyRemoved::new(PermitKey::new("test", 1), PermitEntry::new("test", 1));
        assert_eq!(
            err.to_string(),
            "EarlyRemoved: Semaphore PermitEntry is removed before being acquired: key:PermitKey(test/00000000000000000001) entry:PermitEntry(id:test, n:1)"
        );
    }
}
