// Copyright 2021 Datafuse Labs
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

use pyo3::prelude::*;

#[ctor::ctor]
pub(crate) static RUNTIME: tokio::runtime::Runtime = tokio::runtime::Builder::new_multi_thread()
    .enable_all()
    .build()
    .unwrap();

/// Utility to collect rust futures with GIL released
pub(crate) fn wait_for_future<F: std::future::Future>(py: Python, f: F) -> F::Output
where
    F: Send,
    F::Output: Send,
{
    py.allow_threads(|| RUNTIME.block_on(f))
}
