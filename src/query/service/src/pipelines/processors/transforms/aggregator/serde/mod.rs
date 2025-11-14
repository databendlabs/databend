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

mod serde_meta;
mod transform_aggregate_serializer;
mod transform_aggregate_spill_writer;
mod transform_deserializer;
mod transform_exchange_aggregate_serializer;
mod transform_exchange_async_barrier;
mod transform_spill_reader;

pub use serde_meta::*;
pub use transform_aggregate_serializer::*;
pub use transform_aggregate_spill_writer::*;
pub use transform_deserializer::*;
pub use transform_exchange_aggregate_serializer::*;
pub use transform_exchange_async_barrier::*;
pub use transform_spill_reader::*;

pub mod exchange_defines {
    use arrow_ipc::writer::IpcWriteOptions;
    use arrow_schema::Schema;
    use databend_common_expression::types::DataType;
    use databend_common_expression::types::NumberDataType;
    use databend_common_expression::DataField;
    use databend_common_expression::DataSchema;

    pub fn spilled_schema() -> DataSchema {
        DataSchema::new(vec![
            DataField::new("bucket", DataType::Number(NumberDataType::Int64)),
            DataField::new("data_range_start", DataType::Number(NumberDataType::UInt64)),
            DataField::new("data_range_end", DataType::Number(NumberDataType::UInt64)),
            DataField::new(
                "columns_layout",
                DataType::Array(Box::new(DataType::Number(NumberDataType::UInt64))),
            ),
        ])
    }

    pub fn spilled_arrow_schema() -> Schema {
        let schema = spilled_schema();
        Schema::from(&schema)
    }

    pub fn new_spilled_schema() -> DataSchema {
        DataSchema::new(vec![
            DataField::new("bucket", DataType::Number(NumberDataType::Int64)),
            DataField::new("location", DataType::String),
            DataField::new("row_group", DataType::Binary),
        ])
    }

    pub fn new_spilled_arrow_schema() -> Schema {
        let schema = new_spilled_schema();
        Schema::from(&schema)
    }

    pub fn spilled_write_options() -> IpcWriteOptions {
        IpcWriteOptions::default()
    }
}
