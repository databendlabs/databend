// Copyright Qdrant
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

pub mod simple;
#[cfg(target_arch = "x86_64")]
mod simple_avx;
#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
mod simple_neon;
#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
mod simple_sse;
