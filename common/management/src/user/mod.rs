// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
//

mod user_info;
mod user_mgr;

pub use user_mgr::UserMgrImpl;

#[cfg(test)]
mod user_mgr_test;
