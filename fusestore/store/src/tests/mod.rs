// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[macro_use]
pub mod service;
pub mod logging;

pub use logging::init_tracing;
pub use service::rand_local_addr;
pub use service::start_store_server;
