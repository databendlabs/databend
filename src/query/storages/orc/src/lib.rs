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
#![allow(clippy::uninlined_format_args)]
#![allow(clippy::useless_asref)]
#![allow(clippy::diverging_sub_expression)]
#![feature(try_blocks)]
#![feature(impl_trait_in_assoc_type)]
#![feature(let_chains)]
#![feature(core_intrinsics)]
#![feature(int_roundings)]
#![feature(box_patterns)]

mod chunk_reader_impl;
mod copy_into_table;
mod hashable_schema;
mod processors;
mod read_partition;
mod read_pipeline;
mod strip;
mod table;
mod utils;

pub use copy_into_table::OrcTableForCopy;
pub use processors::decoder::StripeDecoder;
pub use processors::source::ORCSource;
pub use table::OrcTable;
