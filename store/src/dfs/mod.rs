// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
pub mod distributed_fs;

pub use distributed_fs::Dfs;

#[cfg(test)]
mod distributed_fs_test;
