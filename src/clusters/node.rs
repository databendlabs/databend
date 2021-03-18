// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct Node {
    pub id: String,
    pub cpus: usize,
    pub address: String,
}
