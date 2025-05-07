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
#![allow(clippy::large_enum_variant)]
#![allow(clippy::useless_asref)]
#![allow(clippy::diverging_sub_expression)]
#![feature(try_blocks)]
#![feature(impl_trait_in_assoc_type)]
#![feature(let_chains)]
#![feature(core_intrinsics)]
#![feature(int_roundings)]
#![feature(box_patterns)]
// FIXME: Remove this once the deprecated code is removed
#![allow(deprecated)]

mod parquet_part;
mod parquet_rs;
mod read_settings;
mod utils;

pub use parquet_part::ParquetFilePart;
pub use parquet_part::ParquetPart;
pub use parquet_rs::*;
pub use read_settings::ReadSettings;
