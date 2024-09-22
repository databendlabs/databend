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
#![feature(box_patterns)]
#![feature(cursor_remaining)]

mod binary;
mod clickhouse;
mod common_settings;
mod delimiter;
mod field_decoder;
pub mod field_encoder;
mod file_format_type;
pub mod output_format;

pub use clickhouse::ClickhouseFormatType;
pub use delimiter::RecordDelimiter;
pub use field_decoder::*;
pub use file_format_type::parse_timezone;
pub use file_format_type::FileFormatOptionsExt;
pub use file_format_type::FileFormatTypeExt;

pub use crate::common_settings::InputCommonSettings;
pub use crate::common_settings::OutputCommonSettings;
