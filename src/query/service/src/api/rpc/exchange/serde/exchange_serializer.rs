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

use std::any::Any;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use common_arrow::arrow::io::flight::default_ipc_fields;
use common_arrow::arrow::io::flight::serialize_batch;
use common_arrow::arrow::io::flight::WriteOptions;
use common_arrow::arrow::io::ipc::IpcField;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::BlockMetaInfo;
use common_expression::BlockMetaInfoPtr;
use common_expression::DataBlock;
use common_expression::DataSchemaRef;
use common_io::prelude::BinaryWrite;
use common_pipeline_core::pipe::PipeItem;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_transforms::processors::transforms::Transform;
use common_pipeline_transforms::processors::transforms::Transformer;
use serde::Deserializer;
use serde::Serializer;

use crate::api::DataPacket;
use crate::api::FragmentData;

pub struct ExchangeSerializeMeta {
    pub packet: Option<DataPacket>,
}

impl ExchangeSerializeMeta {
    pub fn create(packet: DataPacket) -> BlockMetaInfoPtr {
        Box::new(ExchangeSerializeMeta {
            packet: Some(packet),
        })
    }
}

impl Debug for ExchangeSerializeMeta {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExchangeSerializeMeta").finish()
    }
}

impl serde::Serialize for ExchangeSerializeMeta {
    fn serialize<S>(&self, _: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
        unimplemented!("Unimplemented serialize ExchangeSerializeMeta")
    }
}

impl<'de> serde::Deserialize<'de> for ExchangeSerializeMeta {
    fn deserialize<D>(_: D) -> Result<Self, D::Error>
    where D: Deserializer<'de> {
        unimplemented!("Unimplemented deserialize ExchangeSerializeMeta")
    }
}

#[typetag::serde(name = "exchange_serialize")]
impl BlockMetaInfo for ExchangeSerializeMeta {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn Any {
        self
    }

    fn equals(&self, _: &Box<dyn BlockMetaInfo>) -> bool {
        unimplemented!("Unimplemented equals ExchangeSerializeMeta")
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        unimplemented!("Unimplemented clone ExchangeSerializeMeta")
    }
}

pub struct TransformExchangeSerializer {
    options: WriteOptions,
    ipc_fields: Vec<IpcField>,
}

impl TransformExchangeSerializer {
    pub fn create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        schema: &DataSchemaRef,
    ) -> ProcessorPtr {
        let arrow_schema = schema.to_arrow();
        let ipc_fields = default_ipc_fields(&arrow_schema.fields);
        ProcessorPtr::create(Transformer::create(
            input,
            output,
            TransformExchangeSerializer {
                ipc_fields,
                options: WriteOptions { compression: None },
            },
        ))
    }
}

impl Transform for TransformExchangeSerializer {
    const NAME: &'static str = "ExchangeSerializerTransform";

    fn transform(&mut self, data_block: DataBlock) -> Result<DataBlock> {
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
        )))
    }
}

pub fn create_serializer_item(schema: &DataSchemaRef) -> PipeItem {
    let input = InputPort::create();
    let output = OutputPort::create();

    PipeItem::create(
        TransformExchangeSerializer::create(input.clone(), output.clone(), schema),
        vec![input],
        vec![output],
    )
}

pub fn create_serializer_items(size: usize, schema: &DataSchemaRef) -> Vec<PipeItem> {
    (0..size)
        .into_iter()
        .map(|_| create_serializer_item(schema))
        .collect()
}
