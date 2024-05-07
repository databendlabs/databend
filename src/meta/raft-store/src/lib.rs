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

#![allow(clippy::uninlined_format_args)]
#![feature(coroutines)]
#![feature(impl_trait_in_assoc_type)]
#![feature(lazy_cell)]
#![feature(try_blocks)]
#![allow(clippy::diverging_sub_expression)]

pub mod applier;
pub mod config;
pub mod key_spaces;
pub mod leveled_store;
pub mod log;
pub(crate) mod marked;
pub mod ondisk;
pub mod sm_v003;
pub mod snapshot_config;
pub mod state;
pub mod state_machine;
pub mod utils;
