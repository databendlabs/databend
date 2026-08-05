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

use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_pipeline_transforms::sorts::SortBound;

use crate::servers::flight::v1::exchange::DataExchange;
use crate::servers::flight::v1::exchange::ExchangeInjector;
use crate::servers::flight::v1::scatter::FlightScatter;
use crate::sessions::QueryContext;

pub struct SortInjector {}

impl ExchangeInjector for SortInjector {
    fn flight_scatter(
        &self,
        _: &Arc<QueryContext>,
        exchange: &DataExchange,
    ) -> Result<Arc<dyn FlightScatter>> {
        match exchange {
            DataExchange::NodeToNodeExchange(exchange) => Ok(Arc::new(SortBoundScatter {
                partitions: exchange.destination_ids.len(),
            })),
            _ => unreachable!(),
        }
    }
}

pub struct SortBoundScatter {
    partitions: usize,
}

impl FlightScatter for SortBoundScatter {
    fn name(&self) -> &'static str {
        "SortBound"
    }

    fn execute(&self, data_block: DataBlock) -> Result<Vec<DataBlock>> {
        bound_scatter(data_block, self.partitions)
    }
}

fn bound_scatter(data_block: DataBlock, n: usize) -> Result<Vec<DataBlock>> {
    let meta = *data_block
        .get_meta()
        .and_then(SortBound::downcast_ref_from)
        .unwrap();

    let empty = data_block.slice(0..0);
    let mut result = vec![empty; n];
    result[meta.index as usize % n] = data_block;

    Ok(result)
}
