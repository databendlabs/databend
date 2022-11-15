// Copyright 2022 Datafuse Labs.
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

mod bloom;
pub mod filters;
pub mod index_min_max;
pub mod range_filter;

pub use bloom::BlockFilter;
pub use bloom::FilterEvalResult;
use common_expression::types::DataType;
use common_expression::SchemaDataType;
pub use index_min_max::*;
pub use range_filter::*;

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum IndexSchemaVersion {
    V1,
}

pub trait SupportedType {
    fn is_supported_type(data_type: &DataType) -> bool {
        // we support nullable column but Nulls are not added into the bloom filter.
        let inner_type = data_type.remove_nullable();
        matches!(
            inner_type,
            DataType::Number(_) | DataType::Date | DataType::Timestamp | DataType::String
        )
    }

    fn is_supported_schema_type(data_type: &SchemaDataType) -> bool {
        Self::is_supported_type(&data_type.into())
    }
}
