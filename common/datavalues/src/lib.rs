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

//! *Credits to the work of https://github.com/pola-rs/polars, which served as
//! insipration for the crate*
//!

#![feature(generic_associated_types)]
#![feature(trusted_len)]

#[macro_use]
mod macros;

mod utils;

mod columns;
mod data_field;
mod data_group_value;
mod data_schema;
mod data_value;
mod data_value_operator;
mod scalars;
mod types;

/// third partry
pub use chrono;
pub use chrono_tz::Tz;
/// current
pub use columns::*;
pub use data_field::*;
pub use data_schema::*;
pub use data_value::*;
pub use data_value_operator::*;
pub use prelude::*;
pub use scalars::*;
pub use types::*;
pub use utils::*;

pub mod prelude;
