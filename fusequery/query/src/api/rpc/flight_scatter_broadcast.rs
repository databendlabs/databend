// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;
use common_exception::Result;
use common_planners::Expression;

use crate::api::rpc::flight_scatter::FlightScatter;

pub struct BroadcastFlightScatter {
    scattered_size: usize,
}

impl FlightScatter for BroadcastFlightScatter {
    fn try_create(_: DataSchemaRef, _: Option<Expression>, num: usize) -> Result<Self> {
        Ok(BroadcastFlightScatter {
            scattered_size: num,
        })
    }

    fn execute(&self, data_block: &DataBlock) -> Result<Vec<DataBlock>> {
        let mut data_blocks = vec![];
        for _ in 0..self.scattered_size {
            data_blocks.push(data_block.clone());
        }

        Ok(data_blocks)
    }
}
