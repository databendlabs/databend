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

use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_metrics::storage::metrics_inc_row_fetch_input_batches;
use databend_common_pipeline_transforms::processors::AccumulatingTransform;
use log::info;

pub struct RowFetchExchangeCoalescer {
    query_id: String,
    blocks: Vec<DataBlock>,
    rows: usize,
}

impl RowFetchExchangeCoalescer {
    pub fn create(query_id: String) -> Self {
        Self {
            query_id,
            blocks: Vec::new(),
            rows: 0,
        }
    }
}

impl AccumulatingTransform for RowFetchExchangeCoalescer {
    const NAME: &'static str = "RowFetchExchangeCoalescer";

    fn transform(&mut self, data: DataBlock) -> Result<Vec<DataBlock>> {
        if !data.is_empty() {
            self.rows += data.num_rows();
            self.blocks.push(data);
        }
        Ok(vec![])
    }

    fn on_finish(&mut self, output: bool) -> Result<Vec<DataBlock>> {
        if !output || self.blocks.is_empty() {
            self.blocks.clear();
            self.rows = 0;
            return Ok(vec![]);
        }

        let input_batches = self.blocks.len();
        let block = match input_batches {
            1 => self.blocks.pop().unwrap(),
            _ => DataBlock::concat(&self.blocks)?,
        };
        self.blocks.clear();

        Profile::record_usize_profile(ProfileStatisticsName::RowFetchInputBatches, input_batches);
        metrics_inc_row_fetch_input_batches(input_batches as u64);
        info!(
            "RowFetch exchange coalesced query_id={} input_batches={} rows={}",
            self.query_id, input_batches, self.rows
        );
        self.rows = 0;

        Ok(vec![block])
    }
}

#[cfg(test)]
mod tests {
    use databend_common_expression::FromData;
    use databend_common_expression::types::UInt64Type;

    use super::*;

    fn block(values: &[u64]) -> DataBlock {
        DataBlock::new_from_columns(vec![UInt64Type::from_data(values.to_vec())])
    }

    #[test]
    fn coalesces_non_empty_blocks_once() -> Result<()> {
        let mut coalescer = RowFetchExchangeCoalescer::create("test-query".to_string());
        assert!(coalescer.transform(DataBlock::empty())?.is_empty());
        assert!(coalescer.transform(block(&[1, 2]))?.is_empty());
        assert!(coalescer.transform(block(&[3]))?.is_empty());

        let output = coalescer.on_finish(true)?;
        assert_eq!(output.len(), 1);
        assert_eq!(output[0].num_rows(), 3);
        Ok(())
    }

    #[test]
    fn does_not_emit_for_empty_input() -> Result<()> {
        let mut coalescer = RowFetchExchangeCoalescer::create("test-query".to_string());
        assert!(coalescer.transform(DataBlock::empty())?.is_empty());
        assert!(coalescer.on_finish(true)?.is_empty());
        Ok(())
    }
}
