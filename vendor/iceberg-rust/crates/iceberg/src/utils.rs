// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::num::NonZeroUsize;

// Use a default value of 1 as the safest option.
// See https://doc.rust-lang.org/std/thread/fn.available_parallelism.html#limitations
// for more details.
const DEFAULT_PARALLELISM: usize = 1;

/// Uses [`std::thread::available_parallelism`] in order to
/// retrieve an estimate of the default amount of parallelism
/// that should be used. Note that [`std::thread::available_parallelism`]
/// returns a `Result` as it can fail, so here we use
/// a default value instead.
/// Note: we don't use a OnceCell or LazyCell here as there
/// are circumstances where the level of available
/// parallelism can change during the lifetime of an executing
/// process, but this should not be called in a hot loop.
pub(crate) fn available_parallelism() -> NonZeroUsize {
    std::thread::available_parallelism().unwrap_or_else(|_err| {
        // Failed to get the level of parallelism.
        // TODO: log/trace when this fallback occurs.

        // Using a default value.
        NonZeroUsize::new(DEFAULT_PARALLELISM).unwrap()
    })
}
