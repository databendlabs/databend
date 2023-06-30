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

#![allow(clippy::uninlined_format_args)]

#[macro_use]
pub(crate) mod common;
pub(crate) mod proto_conv;
mod user_proto_conv;
mod user_stage;
mod v002_database_meta;
mod v002_share_account_meta;
mod v002_share_meta;
mod v002_table_meta;
mod v005_database_meta;
mod v005_share_meta;
mod v006_copied_file_info;
mod v010_table_meta;
mod v012_table_meta;
mod v023_table_meta;
mod v024_table_meta;
mod v025_user_stage;
mod v026_schema;
mod v027_schema;
mod v028_schema;
mod v029_schema;
mod v030_user_stage;
mod v031_copy_max_file;
mod v032_file_format_params;
mod v033_table_meta;
mod v034_schema;
mod v035_user_stage;
mod v037_index_meta;
mod v038_empty_proto;
mod v039_data_mask;
mod v040_table_meta;
mod v041_virtual_column;
mod v042_s3_stage_new_field;
mod v043_table_statistics;
mod v044_table_meta;
mod v045_background;
mod v046_index_meta;
