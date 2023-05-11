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

use common_arrow::arrow::datatypes::Schema as ArrowSchema;
use common_arrow::arrow::io::flight::default_ipc_fields;
use common_arrow::arrow::io::flight::deserialize_batch;
use common_arrow::arrow::io::flight::deserialize_dictionary;
use common_arrow::arrow::io::ipc::read::Dictionaries;
use common_arrow::arrow::io::ipc::IpcSchema;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::BlockMetaInfo;
use common_expression::BlockMetaInfoDowncast;
use common_expression::BlockMetaInfoPtr;
use common_expression::DataBlock;
use common_expression::DataSchemaRef;
use common_io::prelude::BinaryRead;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_core::processors::Processor;
use serde::Deserializer;
use serde::Serializer;

use crate::api::DataPacket;
use crate::api::FragmentData;

pub struct TransformExchangeDeserializer {
    input: Arc<InputPort>,
    output: Arc<OutputPort>,

    input_data: Option<DataBlock>,
    output_data: Option<DataBlock>,

    schema: DataSchemaRef,
    ipc_schema: IpcSchema,
    arrow_schema: Arc<ArrowSchema>,
}

impl TransformExchangeDeserializer {
    pub fn create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        schema: &DataSchemaRef,
    ) -> ProcessorPtr {
        let arrow_schema = Arc::new(schema.to_arrow());
        let ipc_fields = default_ipc_fields(&arrow_schema.fields);
        let ipc_schema = IpcSchema {
            fields: ipc_fields,
            is_little_endian: true,
        };

        ProcessorPtr::create(Box::new(TransformExchangeDeserializer {
            input,
            output,
            ipc_schema,
            arrow_schema,
            input_data: None,
            output_data: None,
            schema: schema.clone(),
        }))
    }

    fn recv_data(&self, dict: Vec<DataPacket>, fragment_data: FragmentData) -> Result<DataBlock> {
        const ROW_HEADER_SIZE: usize = std::mem::size_of::<u32>();

        let meta = match bincode::deserialize(&fragment_data.get_meta()[ROW_HEADER_SIZE..]) {
            Ok(meta) => Ok(meta),
            Err(_) => Err(ErrorCode::BadBytes(
                "block meta deserialize error when exchange",
            )),
        }?;

        let mut row_count_meta = &fragment_data.get_meta()[..ROW_HEADER_SIZE];
        let row_count: u32 = row_count_meta.read_scalar()?;

        if row_count == 0 {
            return Ok(DataBlock::new_with_meta(vec![], 0, meta));
        }

        let mut dictionaries = Dictionaries::new();

        for dict_packet in dict {
            if let DataPacket::Dictionary(ff) = dict_packet {
                deserialize_dictionary(
                    &ff,
                    &self.arrow_schema.fields,
                    &self.ipc_schema,
                    &mut dictionaries,
                )?;
            }
        }

        let batch = deserialize_batch(
            &fragment_data.data,
            &self.arrow_schema.fields,
            &self.ipc_schema,
            &dictionaries,
        )?;

        let data_block = DataBlock::from_arrow_chunk(&batch, &self.schema)?;

        if data_block.num_columns() == 0 {
            return Ok(DataBlock::new_with_meta(vec![], row_count as usize, meta));
        }

        data_block.add_meta(meta)
    }
}

#[async_trait::async_trait]
impl Processor for TransformExchangeDeserializer {
    fn name(&self) -> String {
        String::from("TransformExchangeDeserializer")
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.output.is_finished() {
            self.input.finish();
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            self.input.set_not_need_data();
            return Ok(Event::NeedConsume);
        }

        if let Some(output_data) = self.output_data.take() {
            self.output.push_data(Ok(output_data));
            return Ok(Event::NeedConsume);
        }

        if self.input_data.is_some() {
            return Ok(Event::Sync);
        }

        if self.input.has_data() {
            let data_block = self.input.pull_data().unwrap()?;

            match data_block.get_meta() {
                None => self.output.push_data(Ok(data_block)),
                Some(block_meta) => match ExchangeDeserializeMeta::downcast_ref_from(block_meta) {
                    None => self.output.push_data(Ok(data_block)),
                    Some(_meta) => {
                        if data_block.num_rows() != 0 {
                            return Err(ErrorCode::Internal("ExchangeDeserializeMeta has rows"));
                        }

                        self.input_data = Some(data_block);
                        return Ok(Event::Sync);
                    }
                },
            }

            return Ok(Event::NeedConsume);
        }

        if self.input.is_finished() {
            self.output.finish();
            return Ok(Event::Finished);
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    fn process(&mut self) -> Result<()> {
        if let Some(mut data) = self.input_data.take() {
            if let Some(block_meta) = data.take_meta() {
                if let Some(mut exchange_meta) = ExchangeDeserializeMeta::downcast_from(block_meta)
                {
                    self.output_data = Some(match exchange_meta.packet.pop().unwrap() {
                        DataPacket::ErrorCode(v) => Err(v),
                        DataPacket::Dictionary(_) => unreachable!(),
                        DataPacket::FetchProgressAndPrecommit => unreachable!(),
                        DataPacket::ProgressAndPrecommit { .. } => unreachable!(),
                        DataPacket::FragmentData(v) => self.recv_data(exchange_meta.packet, v),
                    }?);

                    return Ok(());
                }
            }

            return Err(ErrorCode::Internal(
                "Internal error, exchange source deserializer only recv exchange source meta.",
            ));
        }

        Ok(())
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
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExchangeSourceMeta").finish()
    }
}

impl serde::Serialize for ExchangeDeserializeMeta {
    fn serialize<S>(&self, _: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
        unimplemented!("Unimplemented serialize ExchangeSourceMeta")
    }
}

impl<'de> serde::Deserialize<'de> for ExchangeDeserializeMeta {
    fn deserialize<D>(_: D) -> Result<Self, D::Error>
    where D: Deserializer<'de> {
        unimplemented!("Unimplemented deserialize ExchangeSourceMeta")
    }
}

#[typetag::serde(name = "exchange_source")]
impl BlockMetaInfo for ExchangeDeserializeMeta {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn equals(&self, _: &Box<dyn BlockMetaInfo>) -> bool {
        unimplemented!("Unimplemented equals ExchangeSourceMeta")
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        unimplemented!("Unimplemented clone ExchangeSourceMeta")
    }
}
