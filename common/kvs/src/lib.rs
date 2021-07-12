// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
//

#[cfg(test)]
mod backend_client_test;

mod backend_client;

pub mod backends;
pub use backend_client::BackendClient;
