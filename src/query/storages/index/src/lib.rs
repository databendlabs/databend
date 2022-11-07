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

use common_datavalues::prelude::TypeID;
use common_datavalues::remove_nullable;
use common_datavalues::DataType;
use common_datavalues::DataTypeImpl;

mod bloom;
pub mod filters;
pub mod index_min_max;
pub mod range_filter;

pub use bloom::BlockFilter;
pub use bloom::FilterEvalResult;
pub use index_min_max::*;
pub use range_filter::*;

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum IndexSchemaVersion {
    V1,
}

pub trait SupportedType {
    fn is_supported_type(data_type: &DataTypeImpl) -> bool {
        // we support nullable column but Nulls are not added into the bloom filter.
        let inner_type = remove_nullable(data_type);
        let data_type_id = inner_type.data_type_id();
        matches!(
            data_type_id,
            TypeID::UInt8
                | TypeID::UInt16
                | TypeID::UInt32
                | TypeID::UInt64
                | TypeID::Int8
                | TypeID::Int16
                | TypeID::Int32
                | TypeID::Int64
                | TypeID::Float32
                | TypeID::Float64
                | TypeID::Date
                | TypeID::Timestamp
                | TypeID::Interval
                | TypeID::String
        )
    }
}
