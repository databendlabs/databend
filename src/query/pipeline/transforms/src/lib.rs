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

#![allow(internal_features)]
#![allow(clippy::unconditional_recursion)]
#![feature(core_intrinsics)]
#![feature(int_roundings)]
#![feature(let_chains)]
#![feature(iter_map_windows)]
#![feature(assert_matches)]
#![feature(debug_closure_helpers)]

pub mod processors;
pub use processors::*;
