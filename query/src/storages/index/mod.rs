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

mod bloom_filter;
mod index_min_max;
pub mod range_filter;
pub use bloom_filter::BloomFilter;
pub use bloom_filter::BloomFilterExprEvalResult;
pub use bloom_filter::BloomFilterIndexer;
use common_datavalues::prelude::*;
use common_datavalues::DataType;
use common_datavalues::DataTypeImpl;
use common_datavalues::NullableType;
pub use index_min_max::MinMaxIndex;
pub use range_filter::ClusterKeyInfo;
pub use range_filter::RangeFilter;

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum IndexSchemaVersion {
    V1,
}

pub trait SupportedType {
    /// Returns whether the data type is supported by bloom filter.
    ///
    /// The supported types are most same as Databricks:
    /// https://docs.microsoft.com/en-us/azure/databricks/delta/optimizations/bloom-filters
    ///
    /// "Bloom filters support columns with the following (input) data types: byte, short, int,
    /// long, float, double, date, timestamp, and string."
    ///
    /// Nulls are not added to the Bloom
    /// filter, so any null related filter requires reading the data file. "
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

pub fn remove_nullable(data_type: &DataTypeImpl) -> DataTypeImpl {
    if matches!(data_type.data_type_id(), TypeID::Nullable) {
        let nullable: NullableType = data_type.to_owned().try_into().unwrap();
        return nullable.inner_type().clone();
    }
    data_type.clone()
}
