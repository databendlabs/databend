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

//! A comparable row format inspired by apache/arrow-rs.
//! Use this module to convert column-oriented data into row-oriented data.
//! It's mainly used for sort processors.

mod fixed;
mod row_converter;
mod variable;

pub use fixed::FixedLengthEncoding;
pub use row_converter::RowConverter;
