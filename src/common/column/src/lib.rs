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

#![feature(iter_advance_by)]
#![feature(portable_simd)]
#![allow(clippy::unconditional_recursion)]
#![allow(clippy::non_canonical_partial_ord_impl)]
#![allow(clippy::len_without_is_empty)]
#![allow(dead_code)]
#![feature(trusted_len)]
#![feature(try_blocks)]

pub mod binary;
pub mod binview;
pub mod bitmap;
pub mod buffer;
pub mod error;
pub mod fmt;
pub mod iterator;
pub mod offset;
pub mod types;

#[macro_use]
pub(crate) mod utils;
