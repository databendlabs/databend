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

use std::any::Any;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use common_arrow::arrow::chunk::Chunk;
use common_arrow::arrow::io::flight::default_ipc_fields;
use common_arrow::arrow::io::flight::serialize_batch;
use common_arrow::arrow::io::flight::WriteOptions;
use common_arrow::arrow::io::ipc::IpcField;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::BlockMetaInfo;
use common_expression::BlockMetaInfoPtr;
use common_expression::DataBlock;
use common_io::prelude::BinaryWrite;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_transforms::processors::transforms::BlockMetaTransform;
use common_pipeline_transforms::processors::transforms::BlockMetaTransformer;
use common_pipeline_transforms::processors::transforms::Transform;
use common_pipeline_transforms::processors::transforms::Transformer;
use common_pipeline_transforms::processors::transforms::UnknownMode;
use serde::Deserializer;
use serde::Serializer;

use crate::api::rpc::exchange::exchange_params::MergeExchangeParams;
use crate::api::rpc::exchange::exchange_params::ShuffleExchangeParams;
use crate::api::rpc::exchange::exchange_transform_shuffle::ExchangeShuffleMeta;
use crate::api::DataPacket;
use crate::api::FragmentData;

pub struct ExchangeSerializeMeta {
    pub block_number: isize,
    pub packet: Vec<DataPacket>,
}

impl ExchangeSerializeMeta {
    pub fn create(block_number: isize, packet: Vec<DataPacket>) -> BlockMetaInfoPtr {
        Box::new(ExchangeSerializeMeta {
            packet,
            block_number,
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
        params: &MergeExchangeParams,
    ) -> Result<ProcessorPtr> {
        let arrow_schema = params.schema.to_arrow();
        let ipc_fields = default_ipc_fields(&arrow_schema.fields);
        Ok(ProcessorPtr::create(Transformer::create(
            input,
            output,
            TransformExchangeSerializer {
                ipc_fields,
                options: WriteOptions { compression: None },
            },
        )))
    }
}

impl Transform for TransformExchangeSerializer {
    const NAME: &'static str = "ExchangeSerializerTransform";

    fn transform(&mut self, data_block: DataBlock) -> Result<DataBlock> {
        serialize_block(0, data_block, &self.ipc_fields, &self.options)
    }
}

pub struct TransformScatterExchangeSerializer {
    local_pos: usize,
    options: WriteOptions,
    ipc_fields: Vec<IpcField>,
}

impl TransformScatterExchangeSerializer {
    pub fn create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        params: &ShuffleExchangeParams,
    ) -> Result<ProcessorPtr> {
        let local_id = &params.executor_id;
        let arrow_schema = params.schema.to_arrow();
        let ipc_fields = default_ipc_fields(&arrow_schema.fields);
        Ok(ProcessorPtr::create(BlockMetaTransformer::create(
            input,
            output,
            TransformScatterExchangeSerializer {
                ipc_fields,
                options: WriteOptions { compression: None },
                local_pos: params
                    .destination_ids
                    .iter()
                    .position(|x| x == local_id)
                    .unwrap(),
            },
        )))
    }
}

impl BlockMetaTransform<ExchangeShuffleMeta> for TransformScatterExchangeSerializer {
    const UNKNOWN_MODE: UnknownMode = UnknownMode::Error;
    const NAME: &'static str = "TransformScatterExchangeSerializer";

    fn transform(&mut self, meta: ExchangeShuffleMeta) -> Result<DataBlock> {
        let mut new_blocks = Vec::with_capacity(meta.blocks.len());
        for (index, block) in meta.blocks.into_iter().enumerate() {
            if block.is_empty() {
                new_blocks.push(block);
                continue;
            }

            new_blocks.push(match self.local_pos == index {
                true => block,
                false => serialize_block(0, block, &self.ipc_fields, &self.options)?,
            });
        }

        Ok(DataBlock::empty_with_meta(ExchangeShuffleMeta::create(
            new_blocks,
        )))
    }
}

pub fn serialize_block(
    block_num: isize,
    data_block: DataBlock,
    ipc_field: &[IpcField],
    options: &WriteOptions,
) -> Result<DataBlock> {
    if data_block.is_empty() && data_block.get_meta().is_none() {
        return Ok(DataBlock::empty_with_meta(ExchangeSerializeMeta::create(
            block_num,
            vec![],
        )));
    }

    let mut meta = vec![];
    meta.write_scalar_own(data_block.num_rows() as u32)?;
    bincode::serialize_into(&mut meta, &data_block.get_meta())
        .map_err(|_| ErrorCode::BadBytes("block meta serialize error when exchange"))?;

    let (dict, values) = match data_block.is_empty() {
        true => serialize_batch(&Chunk::new(vec![]), &[], options)?,
        false => {
            let chunks = data_block.try_into()?;
            serialize_batch(&chunks, ipc_field, options)?
        }
    };

    let mut packet = Vec::with_capacity(dict.len() + 1);

    for dict_flight in dict {
        packet.push(DataPacket::Dictionary(dict_flight));
    }

    packet.push(DataPacket::FragmentData(FragmentData::create(meta, values)));
    Ok(DataBlock::empty_with_meta(ExchangeSerializeMeta::create(
        block_num, packet,
    )))
}
