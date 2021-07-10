// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_exception::Result;
use common_flights::Address;

/// Executor metadata.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct ClusterExecutor {
    // Executor name.
    pub name: String,
    // Node priority is in [0,10]
    // larger value means higher priority
    pub priority: u8,
    // Executor address.
    pub address: Address,
    pub local: bool,
    pub sequence: usize,
}

impl ClusterExecutor {
    pub fn create(name: String, priority: u8, address: Address) -> Result<ClusterExecutor> {
        Ok(ClusterExecutor {
            name,
            priority,
            address,
            local: false,
            sequence: 0,
        })
    }

    pub fn is_local(&self) -> bool {
        self.local
    }
}

impl PartialEq for ClusterExecutor {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.priority == other.priority
            && self.address == other.address
            && self.local == other.local
    }
}
