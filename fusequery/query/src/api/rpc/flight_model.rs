// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

// Fetch partition request.
#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct FetchPartitionRequest {
    pub uuid: String,
    pub nums: u32,
}
