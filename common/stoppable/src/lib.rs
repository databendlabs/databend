// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

mod stop_handle;
mod stoppable;

pub use stop_handle::StopHandle;
pub use stoppable::Stoppable;

#[cfg(test)]
mod stoppable_test;
