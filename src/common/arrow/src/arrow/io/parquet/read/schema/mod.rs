// Copyright 2020-2022 Jorge C. Leitão
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

//! APIs to handle Parquet <-> Arrow schemas.
use crate::arrow::datatypes::Schema;
use crate::arrow::datatypes::TimeUnit;
use crate::arrow::error::Result;

mod convert;
mod metadata;

pub use convert::parquet_to_arrow_schema;
pub use convert::parquet_to_arrow_schema_with_options;
pub(crate) use convert::*;
pub use metadata::read_schema_from_metadata;
pub use parquet2::metadata::FileMetaData;
pub use parquet2::metadata::KeyValue;
pub use parquet2::metadata::SchemaDescriptor;
pub use parquet2::schema::types::ParquetType;

use self::metadata::parse_key_value_metadata;

/// Options when inferring schemas from Parquet
pub struct SchemaInferenceOptions {
    /// When inferring schemas from the Parquet INT96 timestamp type, this is the corresponding TimeUnit
    /// in the inferred Arrow Timestamp type.
    ///
    /// This defaults to `TimeUnit::Nanosecond`, but INT96 timestamps outside of the range of years 1678-2262,
    /// will overflow when parsed as `Timestamp(TimeUnit::Nanosecond)`. Setting this to a lower resolution
    /// (e.g. TimeUnit::Milliseconds) will result in loss of precision, but support a larger range of dates
    /// without overflowing when parsing the data.
    pub int96_coerce_to_timeunit: TimeUnit,
}

impl Default for SchemaInferenceOptions {
    fn default() -> Self {
        SchemaInferenceOptions {
            int96_coerce_to_timeunit: TimeUnit::Nanosecond,
        }
    }
}

/// Infers a [`Schema`] from parquet's [`FileMetaData`]. This first looks for the metadata key
/// `"ARROW:schema"`; if it does not exist, it converts the parquet types declared in the
/// file's parquet schema to Arrow's equivalent.
/// # Error
/// This function errors iff the key `"ARROW:schema"` exists but is not correctly encoded,
/// indicating that that the file's arrow metadata was incorrectly written.
pub fn infer_schema(file_metadata: &FileMetaData) -> Result<Schema> {
    infer_schema_with_options(file_metadata, &None)
}

/// Like [`infer_schema`] but with configurable options which affects the behavior of inference
pub fn infer_schema_with_options(
    file_metadata: &FileMetaData,
    options: &Option<SchemaInferenceOptions>,
) -> Result<Schema> {
    let mut metadata = parse_key_value_metadata(file_metadata.key_value_metadata());

    let schema = read_schema_from_metadata(&mut metadata)?;
    Ok(schema.unwrap_or_else(|| {
        let fields = parquet_to_arrow_schema_with_options(file_metadata.schema().fields(), options);
        Schema { fields, metadata }
    }))
}
