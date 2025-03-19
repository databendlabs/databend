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

use std::sync::Arc;

use arrow_schema::Schema as ArrowSchema;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoPtr;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_io::prelude::BinaryRead;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_transforms::processors::BlockMetaTransform;
use databend_common_pipeline_transforms::processors::BlockMetaTransformer;
use databend_common_pipeline_transforms::processors::UnknownMode;

use crate::servers::flight::v1::exchange::serde::deserialize_block;
use crate::servers::flight::v1::exchange::serde::ExchangeDeserializeMeta;
use crate::servers::flight::v1::packets::DataPacket;
use crate::servers::flight::v1::packets::FragmentData;

pub struct TransformDeserializer {
    schema: DataSchemaRef,
    arrow_schema: Arc<ArrowSchema>,
}

impl TransformDeserializer {
    pub fn try_create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        schema: &DataSchemaRef,
    ) -> Result<ProcessorPtr> {
        let arrow_schema = ArrowSchema::from(schema.as_ref());

        Ok(ProcessorPtr::create(BlockMetaTransformer::create(
            input,
            output,
            TransformDeserializer {
                arrow_schema: Arc::new(arrow_schema),
                schema: schema.clone(),
            },
        )))
    }

    fn recv_data(&self, dict: Vec<DataPacket>, fragment_data: FragmentData) -> Result<DataBlock> {
        const ROW_HEADER_SIZE: usize = std::mem::size_of::<u32>();

        let meta: Option<BlockMetaInfoPtr> =
            serde_json::from_slice(&fragment_data.get_meta()[ROW_HEADER_SIZE..])
                .map_err(|_| ErrorCode::BadBytes("block meta deserialize error when exchange"))?;

        let mut row_count_meta = &fragment_data.get_meta()[..ROW_HEADER_SIZE];
        let row_count: u32 = row_count_meta.read_scalar()?;

        if row_count == 0 {
            return Ok(DataBlock::new_with_meta(vec![], 0, meta));
        }

        let data_block =
            deserialize_block(dict, fragment_data, &self.schema, self.arrow_schema.clone())?;

        match data_block.num_columns() == 0 {
            true => Ok(DataBlock::new_with_meta(vec![], row_count as usize, meta)),
            false => data_block.add_meta(meta),
        }
    }
}

impl BlockMetaTransform<ExchangeDeserializeMeta> for TransformDeserializer {
    const UNKNOWN_MODE: UnknownMode = UnknownMode::Pass;
    const NAME: &'static str = "TransformDeserializer";

    fn transform(&mut self, mut meta: ExchangeDeserializeMeta) -> Result<Vec<DataBlock>> {
        match meta.packet.pop().unwrap() {
            DataPacket::ErrorCode(v) => Err(v),
            DataPacket::Dictionary(_) => unreachable!(),
            DataPacket::QueryProfiles(_) => unreachable!(),
            DataPacket::SerializeProgress { .. } => unreachable!(),
            DataPacket::CopyStatus { .. } => unreachable!(),
            DataPacket::MutationStatus { .. } => unreachable!(),
            DataPacket::DataCacheMetrics(_) => unreachable!(),
            DataPacket::FragmentData(v) => Ok(vec![self.recv_data(meta.packet, v)?]),
        }
    }
}

pub type TransformAggregateDeserializer = TransformDeserializer;
