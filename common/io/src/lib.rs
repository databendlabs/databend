// Copyright 2021 Datafuse Labs.
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

// https://github.com/rust-lang/rust-clippy/issues/8334
#![allow(clippy::ptr_arg)]
#![feature(can_vector)]
#![feature(read_buf)]
#![feature(slice_internals)]
#![feature(maybe_uninit_slice)]
#![feature(new_uninit)]

pub mod prelude;

mod binary_read;
mod binary_write;

mod buffer;
mod configs;
mod files;
mod format_settings;
mod marshal;
mod operator;
mod options_deserializer;
mod stat_buffer;
mod unmarshal;
mod utils;
mod position;
