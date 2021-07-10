// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
//

#[cfg(test)]
mod backend_local_test;

mod backend_local;
mod backend_store;

pub use backend_local::LocalBackend;
pub use backend_store::StoreBackend;
