// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

pub use ifs::IFileSystem;
pub use list_result::ListResult;
pub use localfs::LocalFS;

pub mod ifs;
mod list_result;
pub mod localfs;

#[cfg(test)]
mod localfs_test;
