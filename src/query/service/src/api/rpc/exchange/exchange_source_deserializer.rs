//  Copyright 2023 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::sync::Arc;

use common_arrow::arrow::datatypes::Schema as ArrowSchema;
use common_arrow::arrow::io::flight::default_ipc_fields;
use common_arrow::arrow::io::flight::deserialize_batch;
use common_arrow::arrow::io::ipc::IpcSchema;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::DataBlock;
use common_expression::DataSchemaRef;
use common_io::prelude::BinaryRead;
use common_pipeline_core::pipe::Pipe;
use common_pipeline_core::pipe::PipeItem;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_core::Pipeline;
use common_pipeline_transforms::processors::transforms::Transform;
use common_pipeline_transforms::processors::transforms::Transformer;

use crate::api::rpc::exchange::exchange_params::MergeExchangeParams;
use crate::api::rpc::exchange::exchange_source::ExchangeSourceMeta;
use crate::api::DataPacket;
use crate::api::FragmentData;
use crate::pipelines::processors::TransformDummy;

pub struct TransformExchangeDeserializer {
    schema: DataSchemaRef,
    ipc_schema: IpcSchema,
    arrow_schema: Arc<ArrowSchema>,
}

impl TransformExchangeDeserializer {
    pub fn create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        params: &MergeExchangeParams,
    ) -> ProcessorPtr {
        let arrow_schema = Arc::new(params.schema.to_arrow());
        let ipc_fields = default_ipc_fields(&arrow_schema.fields);
        let ipc_schema = IpcSchema {
            fields: ipc_fields,
            is_little_endian: true,
        };

        Transformer::create(input, output, TransformExchangeDeserializer {
            ipc_schema,
            arrow_schema,
            schema: params.schema.clone(),
        })
    }

    fn recv_data(&self, fragment_data: FragmentData) -> Result<DataBlock> {
        let batch = deserialize_batch(
            &fragment_data.data,
            &self.arrow_schema.fields,
            &self.ipc_schema,
            &Default::default(),
        )?;

        const ROW_HEADER_SIZE: usize = std::mem::size_of::<u32>();

        let meta = match bincode::deserialize(&fragment_data.get_meta()[ROW_HEADER_SIZE..]) {
            Ok(meta) => Ok(meta),
            Err(_) => Err(ErrorCode::BadBytes(
                "block meta deserialize error when exchange",
            )),
        }?;

        let block = DataBlock::from_arrow_chunk(&batch, &self.schema)?;

        if block.num_columns() == 0 {
            let mut row_count_meta = &fragment_data.get_meta()[..ROW_HEADER_SIZE];
            let row_count: u32 = row_count_meta.read_scalar()?;
            return Ok(DataBlock::new_with_meta(vec![], row_count as usize, meta));
        }

        block.add_meta(meta)
    }
}

impl Transform for TransformExchangeDeserializer {
    const NAME: &'static str = "TransformExchangeDeserializer";

    fn transform(&mut self, mut data: DataBlock) -> Result<DataBlock> {
        if let Some(mut block_meta) = data.take_meta() {
            if let Some(exchange_meta) =
                block_meta.as_mut_any().downcast_mut::<ExchangeSourceMeta>()
            {
                return match exchange_meta.packet.take().unwrap() {
                    DataPacket::ErrorCode(v) => Err(v),
                    DataPacket::ClosingClient => unreachable!(),
                    DataPacket::FetchProgressAndPrecommit => unreachable!(),
                    DataPacket::ProgressAndPrecommit { .. } => unreachable!(),
                    DataPacket::FragmentData(v) => self.recv_data(v),
                };
            }
        }

        Err(ErrorCode::Internal(
            "Internal error, exchange source deserializer only recv exchange source meta.",
        ))
    }
}

pub fn via_deserializer(
    prefix_size: usize,
    suffix_size: usize,
    params: &MergeExchangeParams,
    pipeline: &mut Pipeline,
) {
    let mut items = Vec::with_capacity(prefix_size + suffix_size);

    for _index in 0..prefix_size {
        let input = InputPort::create();
        let output = OutputPort::create();

        items.push(PipeItem::create(
            TransformDummy::create(input.clone(), output.clone()),
            vec![input],
            vec![output],
        ));
    }

    for _index in 0..suffix_size {
        let input = InputPort::create();
        let output = OutputPort::create();

        items.push(PipeItem::create(
            TransformExchangeDeserializer::create(input.clone(), output.clone(), params),
            vec![input],
            vec![output],
        ));
    }

    pipeline.add_pipe(Pipe::create(items.len(), items.len(), items));
}
