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
use arrow_schema::ArrowError;
use arrow_schema::Schema as ArrowSchema;
use bytes::Bytes;
use databend_common_exception::Result;
use databend_common_expression::local_block_meta_serde;
use databend_common_expression::BlockMetaInfo;
use databend_common_expression::BlockMetaInfoPtr;
use databend_common_expression::DataBlock;
use databend_common_io::prelude::BinaryWrite;

use crate::servers::flight::v1::packets::DataPacket;
use crate::servers::flight::v1::packets::FragmentData;

pub struct ExchangeSerializeMeta {
    pub partition: isize,
    pub max_partition: usize,
    pub global_max_partition: usize,
    pub packet: Vec<DataPacket>,
}

impl ExchangeSerializeMeta {
    pub fn create(
        partition: isize,
        max_partition: usize,
        global_max_partition: usize,
        packet: Vec<DataPacket>,
    ) -> BlockMetaInfoPtr {
        Box::new(ExchangeSerializeMeta {
            packet,
            partition,
            max_partition,
            global_max_partition,
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

pub fn serialize_block(
    partition: isize,
    max_partition: usize,
    global_max_partition: usize,
    data_block: DataBlock,
    options: &IpcWriteOptions,
) -> Result<DataBlock> {
    if data_block.is_empty() && data_block.get_meta().is_none() {
        return Ok(DataBlock::empty_with_meta(ExchangeSerializeMeta::create(
            partition,
            max_partition,
            global_max_partition,
            vec![],
        )));
    }

    let mut meta = vec![];
    meta.write_scalar_own(data_block.num_rows() as u32)?;
    serde_json::to_writer(&mut meta, &data_block.get_meta())?;

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
        partition,
        max_partition,
        global_max_partition,
        packet,
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
