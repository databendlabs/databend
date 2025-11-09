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

use arrow_array::RecordBatch;
use arrow_array::RecordBatchOptions;
use arrow_flight::FlightData;
use arrow_flight::SchemaAsIpc;
use arrow_ipc::writer::DictionaryTracker;
use arrow_ipc::writer::IpcDataGenerator;
use arrow_ipc::writer::IpcWriteOptions;
use arrow_ipc::CompressionType;
use arrow_schema::ArrowError;
use arrow_schema::Schema as ArrowSchema;
use bytes::Bytes;
use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::local_block_meta_serde;
use databend_common_expression::BlockMetaInfo;
use databend_common_expression::BlockMetaInfoPtr;
use databend_common_expression::DataBlock;
use databend_common_io::prelude::bincode_serialize_into_buf;
use databend_common_io::prelude::BinaryWrite;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline_transforms::processors::BlockMetaTransform;
use databend_common_pipeline_transforms::processors::BlockMetaTransformer;
use databend_common_pipeline_transforms::processors::Transform;
use databend_common_pipeline_transforms::processors::Transformer;
use databend_common_pipeline_transforms::processors::UnknownMode;
use databend_common_settings::FlightCompression;

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

local_block_meta_serde!(ExchangeSerializeMeta);

#[typetag::serde(name = "exchange_serialize")]
impl BlockMetaInfo for ExchangeSerializeMeta {}

pub struct TransformExchangeSerializer {
    options: IpcWriteOptions,
}

impl TransformExchangeSerializer {
    pub fn create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        _params: &MergeExchangeParams,
        compression: Option<FlightCompression>,
    ) -> Result<ProcessorPtr> {
        let compression = match compression {
            None => None,
            Some(compression) => match compression {
                FlightCompression::Lz4 => Some(CompressionType::LZ4_FRAME),
                FlightCompression::Zstd => Some(CompressionType::ZSTD),
            },
        };

        Ok(ProcessorPtr::create(Transformer::create(
            input,
            output,
            TransformExchangeSerializer {
                options: IpcWriteOptions::default().try_with_compression(compression)?,
            },
        )))
    }
}

impl Transform for TransformExchangeSerializer {
    const NAME: &'static str = "ExchangeSerializerTransform";

    fn transform(&mut self, data_block: DataBlock) -> Result<DataBlock> {
        Profile::record_usize_profile(ProfileStatisticsName::ExchangeRows, data_block.num_rows());
        serialize_block(0, data_block, &self.options)
    }
}

pub struct TransformScatterExchangeSerializer {
    local_pos: usize,
    options: IpcWriteOptions,
}

impl TransformScatterExchangeSerializer {
    pub fn create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        compression: Option<FlightCompression>,
        params: &ShuffleExchangeParams,
    ) -> Result<ProcessorPtr> {
        let local_id = &params.executor_id;
        let compression = match compression {
            None => None,
            Some(compression) => match compression {
                FlightCompression::Lz4 => Some(CompressionType::LZ4_FRAME),
                FlightCompression::Zstd => Some(CompressionType::ZSTD),
            },
        };

        Ok(ProcessorPtr::create(BlockMetaTransformer::create(
            input,
            output,
            TransformScatterExchangeSerializer {
                options: IpcWriteOptions::default().try_with_compression(compression)?,
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
            new_blocks.push(match self.local_pos == index {
                true => block,
                false => serialize_block(0, block, &self.options)?,
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
    options: &IpcWriteOptions,
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

    let (_, dict, values) = match data_block.is_empty() {
        true => batches_to_flight_data_with_options(
            &ArrowSchema::empty(),
            vec![RecordBatch::try_new_with_options(
                Arc::new(ArrowSchema::empty()),
                vec![],
                &RecordBatchOptions::new().with_row_count(Some(0)),
            )
            .unwrap()],
            options,
        )?,
        false => {
            let schema = data_block.infer_schema();
            let arrow_schema = ArrowSchema::from(&schema);
            let batch = data_block.to_record_batch_with_dataschema(&schema)?;
            batches_to_flight_data_with_options(&arrow_schema, vec![batch], options)?
        }
    };

    let mut packet = Vec::with_capacity(dict.len() + values.len());
    for dict_flight in dict {
        packet.push(DataPacket::Dictionary(dict_flight));
    }

    let meta: Bytes = meta.into();
    for value in values {
        packet.push(DataPacket::FragmentData(FragmentData::create(
            meta.clone(),
            value,
        )));
    }

    Ok(DataBlock::empty_with_meta(ExchangeSerializeMeta::create(
        block_num, packet,
    )))
}

/// Convert `RecordBatch`es to wire protocol `FlightData`s
/// Returns schema, dictionaries and flight data
pub fn batches_to_flight_data_with_options(
    schema: &ArrowSchema,
    batches: Vec<RecordBatch>,
    options: &IpcWriteOptions,
) -> std::result::Result<(FlightData, Vec<FlightData>, Vec<FlightData>), ArrowError> {
    let schema_flight_data: FlightData = SchemaAsIpc::new(schema, options).into();
    let mut dictionaries = Vec::with_capacity(batches.len());
    let mut flight_data = Vec::with_capacity(batches.len());

    let data_gen = IpcDataGenerator::default();
    let mut dictionary_tracker = DictionaryTracker::new(false);

    for batch in batches.iter() {
        let (encoded_dictionaries, encoded_batch) =
            data_gen.encoded_batch(batch, &mut dictionary_tracker, options)?;

        dictionaries.extend(encoded_dictionaries.into_iter().map(Into::into));
        flight_data.push(encoded_batch.into());
    }
    Ok((schema_flight_data, dictionaries, flight_data))
}
