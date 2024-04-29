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

use databend_common_exception::Result;
use databend_common_expression::DataBlock;

use crate::servers::flight::v1::scatter::flight_scatter::FlightScatter;

pub struct BroadcastFlightScatter {
    scattered_size: usize,
}

impl BroadcastFlightScatter {
    pub fn try_create(scattered_size: usize) -> Result<Self> {
        Ok(BroadcastFlightScatter { scattered_size })
    }
}

impl FlightScatter for BroadcastFlightScatter {
    fn execute(&self, data_block: DataBlock) -> Result<Vec<DataBlock>> {
        let mut data_blocks = vec![];
        for _ in 0..self.scattered_size {
            data_blocks.push(data_block.clone());
        }

        Ok(data_blocks)
    }
}
