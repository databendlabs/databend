// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

mod logging;

pub use logging::init_default_tracing;
pub use logging::init_tracing_with_level;
pub use tracing;
