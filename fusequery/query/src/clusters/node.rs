// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct Node {
    pub name: String,
    pub cpus: usize,
    // Node priority is in [0,10]
    // larger value means higher priority
    pub priority: u8,
    pub address: String,
    pub local: bool,
}

impl Node {
    pub fn is_local(&self) -> bool {
        self.local
    }
}
