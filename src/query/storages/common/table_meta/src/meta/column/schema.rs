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

use std::sync::Arc;

use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::Fields;
use arrow::datatypes::Schema;
use arrow::datatypes::SchemaRef;

pub fn location_fields() -> Fields {
    Fields::from(vec![
        Field::new("path", DataType::Utf8, false),
        Field::new("format_version", DataType::UInt64, false),
    ])
}

pub fn location_type() -> DataType {
    DataType::Struct(location_fields())
}

pub fn segment_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("row_count", DataType::UInt64, false),
        Field::new("block_size", DataType::UInt64, false),
        Field::new("file_size", DataType::UInt64, false),
        Field::new("location", location_type(), false),
        // Field::new("bloom_filter_index_location", location_type(), true),
        Field::new("bloom_filter_index_size", DataType::UInt64, false),
        Field::new("inverted_index_size", DataType::UInt64, true),
        Field::new("compression", DataType::UInt8, false),
    ]))
}
