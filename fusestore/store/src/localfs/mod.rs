// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

pub mod local_fs;

pub use local_fs::LocalFS;

#[cfg(test)]
mod local_fs_test;
