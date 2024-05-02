// Copyright 2022 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(clippy::uninlined_format_args)]
#![feature(try_blocks)]
#![feature(trait_alias)]
#![feature(iter_collect_into)]
#![allow(clippy::arc_with_non_send_sync)]

use once_cell::sync::Lazy;
use parking_lot::Mutex;

// We can generate new test files via using `env REGENERATE_GOLDENFILES=1 cargo test` and `git diff` to show differs
mod aggregates;
mod scalars;

static TRACING_INITIALIZED: Lazy<Mutex<()>> = Lazy::new(|| {
    // This code block is executed only once, regardless of how many times it's called
    env_logger::init();
    Mutex::new(())
});

pub(crate) fn ensure_tracing_initialized() {
    // The lock is acquired here to ensure thread-safe access, but since we're only
    // interested in initializing the tracing subscriber, which itself is thread-safe
    // and meant to be called once, the lock is not used further.
    let _guard = TRACING_INITIALIZED.lock();
    // At this point, tracing_subscriber::fmt::init() has already been called once.
}
