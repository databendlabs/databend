// Copyright 2023 Datafuse Labs.
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

use common_arrow::arrow::io::flight::default_ipc_fields;
use common_arrow::arrow::io::flight::serialize_batch;
use common_arrow::arrow::io::flight::WriteOptions;
use common_arrow::arrow::io::ipc::IpcField;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::DataBlock;
use common_expression::DataSchemaRef;
use common_io::prelude::BinaryWrite;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_transforms::processors::transforms::Transform;
use common_pipeline_transforms::processors::transforms::Transformer;

use crate::api::rpc::exchange::exchange_sorting::ExchangeSorting;
use crate::api::rpc::exchange::serde::exchange_serializer::ExchangeSerializeMeta;
use crate::api::DataPacket;
use crate::api::FragmentData;

pub struct TransformExchangeSerializerWithSorting {
    options: WriteOptions,
    ipc_fields: Vec<IpcField>,
    sorting: Arc<dyn ExchangeSorting>,
}

impl TransformExchangeSerializerWithSorting {
    pub fn create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        schema: &DataSchemaRef,
        sorting: Arc<dyn ExchangeSorting>,
    ) -> ProcessorPtr {
        let arrow_schema = schema.to_arrow();
        let ipc_fields = default_ipc_fields(&arrow_schema.fields);
        ProcessorPtr::create(Transformer::create(
            input,
            output,
            TransformExchangeSerializerWithSorting {
                ipc_fields,
                options: WriteOptions { compression: None },
                sorting,
            },
        ))
    }
}

impl Transform for TransformExchangeSerializerWithSorting {
    const NAME: &'static str = "ExchangeSerializerTransform";

    fn transform(&mut self, data_block: DataBlock) -> Result<DataBlock> {
        let block_number = self.sorting.block_number(&data_block)?;
        let mut meta = vec![];
        meta.write_scalar_own(data_block.num_rows() as u32)?;
        bincode::serialize_into(&mut meta, &data_block.get_meta())
            .map_err(|_| ErrorCode::BadBytes("block meta serialize error when exchange"))?;

        let chunks = data_block.try_into()?;
        let (dicts, values) = serialize_batch(&chunks, &self.ipc_fields, &self.options)?;

        if !dicts.is_empty() {
            return Err(ErrorCode::Unimplemented(
                "DatabendQuery does not implement dicts.",
            ));
        }

        Ok(DataBlock::empty_with_meta(ExchangeSerializeMeta::create(
            DataPacket::FragmentData(FragmentData::create(meta, values)),
            block_number,
        )))
    }
}
