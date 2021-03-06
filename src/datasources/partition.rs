// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

pub type Partitions = Vec<Partition>;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct Partition {
    pub name: String,
    pub version: u64,
}
