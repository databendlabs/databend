// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
//

#[cfg(test)]
mod backend_local_test;

mod backend;
mod backend_local;
mod backend_memory;
mod backend_store;

pub use backend::Backend;
pub use backend::Lock;
pub use backend_local::LocalBackend;
pub use backend_memory::MemoryBackend;
pub use backend_store::StoreBackend;
