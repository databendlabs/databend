// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
//

pub(crate) mod user_api;
pub(crate) mod user_mgr;
///
///
///
/// let mut um = UserMgr::new(client);
/// let a = "test";
/// um.get_users(&vec![a]);
///
pub(crate) mod utils;

#[cfg(test)]
mod user_mgr_test;
