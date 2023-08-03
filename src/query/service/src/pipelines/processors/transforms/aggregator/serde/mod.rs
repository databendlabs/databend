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
mod transform_exchange_group_by_serializer;
mod transform_group_by_serializer;
mod transform_group_by_spill_writer;
mod transform_spill_reader;

pub use serde_meta::AggregateSerdeMeta;
pub use serde_meta::BUCKET_TYPE;
pub use serde_meta::SPILLED_TYPE;
pub use transform_aggregate_serializer::TransformAggregateSerializer;
pub use transform_aggregate_spill_writer::TransformAggregateSpillWriter;
pub use transform_deserializer::TransformAggregateDeserializer;
pub use transform_deserializer::TransformGroupByDeserializer;
pub use transform_exchange_aggregate_serializer::TransformExchangeAggregateSerializer;
pub use transform_exchange_async_barrier::TransformExchangeAsyncBarrier;
pub use transform_exchange_group_by_serializer::TransformExchangeGroupBySerializer;
pub use transform_group_by_serializer::TransformGroupBySerializer;
pub use transform_group_by_spill_writer::TransformGroupBySpillWriter;
pub use transform_spill_reader::TransformAggregateSpillReader;
pub use transform_spill_reader::TransformGroupBySpillReader;

pub mod exchange_defines {
    use common_arrow::arrow::datatypes::Field;
    use common_arrow::arrow::io::flight::default_ipc_fields;
    use common_arrow::arrow::io::flight::WriteOptions;
    use common_arrow::arrow::io::ipc::IpcField;
    use common_arrow::arrow::io::ipc::IpcSchema;
    use common_expression::types::DataType;
    use common_expression::types::NumberDataType;
    use common_expression::DataField;
    use common_expression::DataSchema;
    use once_cell::sync::OnceCell;

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

    pub fn spilled_fields() -> &'static [Field] {
        static IPC_SCHEMA: OnceCell<Vec<Field>> = OnceCell::new();

        IPC_SCHEMA.get_or_init(|| {
            let schema = spilled_schema();

            schema.to_arrow().fields
        })
    }

    pub fn spilled_ipc_schema() -> &'static IpcSchema {
        static IPC_SCHEMA: OnceCell<IpcSchema> = OnceCell::new();

        IPC_SCHEMA.get_or_init(|| {
            let schema = spilled_schema();

            let arrow_schema = schema.to_arrow();
            let ipc_fields = default_ipc_fields(&arrow_schema.fields);

            IpcSchema {
                fields: ipc_fields,
                is_little_endian: true,
            }
        })
    }

    pub fn spilled_ipc_fields() -> &'static [IpcField] {
        static IPC_FIELDS: OnceCell<Vec<IpcField>> = OnceCell::new();

        IPC_FIELDS.get_or_init(|| {
            let schema = spilled_schema();
            let arrow_schema = schema.to_arrow();
            default_ipc_fields(&arrow_schema.fields)
        })
    }

    pub fn spilled_write_options() -> &'static WriteOptions {
        static WRITE_OPTIONS: WriteOptions = WriteOptions { compression: None };
        &WRITE_OPTIONS
    }
}
