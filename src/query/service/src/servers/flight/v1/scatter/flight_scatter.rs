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
use databend_common_expression::BlockPartitionStream;
use databend_common_expression::DataBlock;

pub trait FlightScatter: Sync + Send {
    fn name(&self) -> &'static str;

    fn execute(&self, data_block: DataBlock) -> Result<Vec<DataBlock>>;

    /// Scatter a block into destination blocks. Index-based scatter keeps using
    /// BlockPartitionStream batching; block-level scatter is used when rows may
    /// need to be duplicated across destinations.
    fn scatter_block(
        &self,
        data_block: DataBlock,
        partition_stream: &mut BlockPartitionStream,
    ) -> Result<Vec<(usize, DataBlock)>> {
        let _ = partition_stream;
        Ok(self.execute(data_block)?.into_iter().enumerate().collect())
    }
}
