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

use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use arrow_buffer::Buffer;
use arrow_flight::utils::flight_data_to_arrow_batch;
use arrow_ipc::root_as_message;
use arrow_schema::Schema as ArrowSchema;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::local_block_meta_serde;
use databend_common_expression::BlockMetaInfo;
use databend_common_expression::BlockMetaInfoPtr;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchema;
use databend_common_expression::DataSchemaRef;
use databend_common_io::prelude::bincode_deserialize_from_slice;
use databend_common_io::prelude::BinaryRead;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_transforms::processors::BlockMetaTransform;
use databend_common_pipeline_transforms::processors::BlockMetaTransformer;
use databend_common_pipeline_transforms::processors::UnknownMode;

use crate::servers::flight::v1::packets::DataPacket;
use crate::servers::flight::v1::packets::FragmentData;

pub struct TransformExchangeDeserializer {
    schema: DataSchemaRef,
    arrow_schema: Arc<ArrowSchema>,
}

impl TransformExchangeDeserializer {
    pub fn create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        schema: &DataSchemaRef,
    ) -> ProcessorPtr {
        let arrow_schema = ArrowSchema::from(schema.as_ref());

        ProcessorPtr::create(BlockMetaTransformer::create(
            input,
            output,
            TransformExchangeDeserializer {
                arrow_schema: Arc::new(arrow_schema),
                schema: schema.clone(),
            },
        ))
    }

    fn recv_data(&self, dict: Vec<DataPacket>, fragment_data: FragmentData) -> Result<DataBlock> {
        const ROW_HEADER_SIZE: usize = std::mem::size_of::<u32>();

        let meta = bincode_deserialize_from_slice(&fragment_data.get_meta()[ROW_HEADER_SIZE..])
            .map_err(|_| ErrorCode::BadBytes("block meta deserialize error when exchange"))?;
        let mut row_count_meta = &fragment_data.get_meta()[..ROW_HEADER_SIZE];
        let row_count: u32 = row_count_meta.read_scalar()?;

        if row_count == 0 {
            return Ok(DataBlock::new_with_meta(vec![], 0, meta));
        }

        let data_block =
            deserialize_block(dict, fragment_data, &self.schema, self.arrow_schema.clone())?;
        if data_block.num_columns() == 0 {
            return Ok(DataBlock::new_with_meta(vec![], row_count as usize, meta));
        }
        data_block.add_meta(meta)
    }
}

pub fn deserialize_block(
    dict: Vec<DataPacket>,
    fragment_data: FragmentData,
    schema: &DataSchema,
    arrow_schema: Arc<ArrowSchema>,
) -> Result<DataBlock> {
    let mut dictionaries_by_id = HashMap::new();
    for dict_packet in dict {
        if let DataPacket::Dictionary(data) = dict_packet {
            let message =
                root_as_message(&data.data_header[..]).expect("Error parsing first message");
            let buffer = Buffer::from(data.data_body);
            arrow_ipc::reader::read_dictionary(
                &buffer,
                message
                    .header_as_dictionary_batch()
                    .expect("Error parsing dictionary"),
                &arrow_schema,
                &mut dictionaries_by_id,
                &message.version(),
            )
            .expect("Error reading dictionary");
        }
    }

    let batch = flight_data_to_arrow_batch(&fragment_data.data, arrow_schema, &dictionaries_by_id)?;
    let data_block = DataBlock::from_record_batch(schema, &batch)?;
    Ok(data_block)
}

impl BlockMetaTransform<ExchangeDeserializeMeta> for TransformExchangeDeserializer {
    const UNKNOWN_MODE: UnknownMode = UnknownMode::Pass;
    const NAME: &'static str = "TransformExchangeDeserializer";

    fn transform(&mut self, mut meta: ExchangeDeserializeMeta) -> Result<Vec<DataBlock>> {
        match meta.packet.pop().unwrap() {
            DataPacket::ErrorCode(v) => Err(v),
            DataPacket::Dictionary(_) => unreachable!(),
            DataPacket::SerializeProgress { .. } => unreachable!(),
            DataPacket::CopyStatus { .. } => unreachable!(),
            DataPacket::MutationStatus { .. } => unreachable!(),
            DataPacket::QueryProfiles(_) => unreachable!(),
            DataPacket::DataCacheMetrics(_) => unreachable!(),
            DataPacket::FragmentData(v) => Ok(vec![self.recv_data(meta.packet, v)?]),
            DataPacket::QueryPerf(_) => unreachable!(),
            DataPacket::PartStatistics(_) => unreachable!(),
        }
    }
}

pub struct ExchangeDeserializeMeta {
    pub packet: Vec<DataPacket>,
}

impl ExchangeDeserializeMeta {
    pub fn create(packet: Vec<DataPacket>) -> BlockMetaInfoPtr {
        Box::new(ExchangeDeserializeMeta { packet })
    }
}

impl Debug for ExchangeDeserializeMeta {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        f.debug_struct("ExchangeSourceMeta").finish()
    }
}

local_block_meta_serde!(ExchangeDeserializeMeta);

#[typetag::serde(name = "exchange_source")]
impl BlockMetaInfo for ExchangeDeserializeMeta {}
