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

use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use databend_common_arrow::arrow::chunk::Chunk;
use databend_common_arrow::arrow::datatypes::Schema as ArrowSchema;
use databend_common_arrow::arrow::io::flight::default_ipc_fields;
use databend_common_arrow::arrow::io::flight::serialize_batch;
use databend_common_arrow::arrow::io::flight::WriteOptions;
use databend_common_arrow::arrow::io::ipc::write::Compression;
use databend_common_arrow::arrow::io::ipc::IpcField;
use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfo;
use databend_common_expression::BlockMetaInfoPtr;
use databend_common_expression::DataBlock;
use databend_common_io::prelude::bincode_serialize_into_buf;
use databend_common_io::prelude::BinaryWrite;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_transforms::processors::BlockMetaTransform;
use databend_common_pipeline_transforms::processors::BlockMetaTransformer;
use databend_common_pipeline_transforms::processors::Transform;
use databend_common_pipeline_transforms::processors::Transformer;
use databend_common_pipeline_transforms::processors::UnknownMode;
use databend_common_settings::FlightCompression;
use serde::Deserializer;
use serde::Serializer;

use crate::servers::flight::v1::exchange::ExchangeShuffleMeta;
use crate::servers::flight::v1::exchange::MergeExchangeParams;
use crate::servers::flight::v1::exchange::ShuffleExchangeParams;
use crate::servers::flight::v1::packets::DataPacket;
use crate::servers::flight::v1::packets::FragmentData;

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
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
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
        compression: Option<FlightCompression>,
    ) -> Result<ProcessorPtr> {
        let arrow_schema = ArrowSchema::from(params.schema.as_ref());
        let ipc_fields = default_ipc_fields(&arrow_schema.fields);
        let compression = match compression {
            None => None,
            Some(compression) => match compression {
                FlightCompression::Lz4 => Some(Compression::LZ4),
                FlightCompression::Zstd => Some(Compression::ZSTD),
            },
        };

        Ok(ProcessorPtr::create(Transformer::create(
            input,
            output,
            TransformExchangeSerializer {
                ipc_fields,
                options: WriteOptions { compression },
            },
        )))
    }
}

impl Transform for TransformExchangeSerializer {
    const NAME: &'static str = "ExchangeSerializerTransform";

    fn transform(&mut self, data_block: DataBlock) -> Result<DataBlock> {
        Profile::record_usize_profile(ProfileStatisticsName::ExchangeRows, data_block.num_rows());
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
        compression: Option<FlightCompression>,
        params: &ShuffleExchangeParams,
    ) -> Result<ProcessorPtr> {
        let local_id = &params.executor_id;
        let arrow_schema = ArrowSchema::from(params.schema.as_ref());
        let ipc_fields = default_ipc_fields(&arrow_schema.fields);
        let compression = match compression {
            None => None,
            Some(compression) => match compression {
                FlightCompression::Lz4 => Some(Compression::LZ4),
                FlightCompression::Zstd => Some(Compression::ZSTD),
            },
        };

        Ok(ProcessorPtr::create(BlockMetaTransformer::create(
            input,
            output,
            TransformScatterExchangeSerializer {
                ipc_fields,
                options: WriteOptions { compression },
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

    fn transform(&mut self, meta: ExchangeShuffleMeta) -> Result<Vec<DataBlock>> {
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

        Ok(vec![DataBlock::empty_with_meta(
            ExchangeShuffleMeta::create(new_blocks),
        )])
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
    bincode_serialize_into_buf(&mut meta, &data_block.get_meta())
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
