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

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::local_block_meta_serde;
use databend_common_expression::BlockMetaInfo;
use databend_common_expression::BlockMetaInfoPtr;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::Pipeline;

use super::exchange_params::ShuffleExchangeParams;
use crate::pipelines::processors::transforms::aggregator::FlightExchange;
use crate::sessions::QueryContext;

pub struct ExchangeShuffleMeta {
    pub blocks: Vec<DataBlock>,
}

impl ExchangeShuffleMeta {
    pub fn create(blocks: Vec<DataBlock>) -> BlockMetaInfoPtr {
        Box::new(ExchangeShuffleMeta { blocks })
    }
}

impl Debug for ExchangeShuffleMeta {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        f.debug_struct("ExchangeShuffleMeta").finish()
    }
}

local_block_meta_serde!(ExchangeShuffleMeta);

#[typetag::serde(name = "exchange_shuffle")]
impl BlockMetaInfo for ExchangeShuffleMeta {}

// Scatter the data block and push it to the corresponding output port
pub fn exchange_shuffle(
    ctx: &Arc<QueryContext>,
    params: &ShuffleExchangeParams,
    pipeline: &mut Pipeline,
) -> Result<()> {
    if let Some(last_pipe) = pipeline.pipes.last() {
        for item in &last_pipe.items {
            item.processor.configure_peer_nodes(&params.destination_ids);
        }
    }

    let settings = ctx.get_settings();
    let compression = settings.get_query_flight_compression()?;

    match params.enable_multiway_sort {
        true => pipeline.exchange(
            params.destination_ids.len(),
            FlightExchange::<true>::create(
                params.destination_ids.clone(),
                compression,
                params.shuffle_scatter.clone(),
            ),
        ),
        false => pipeline.exchange(
            params.destination_ids.len(),
            FlightExchange::<false>::create(
                params.destination_ids.clone(),
                compression,
                params.shuffle_scatter.clone(),
            ),
        ),
    };

    Ok(())
}
