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

use std::ops::Range;

use databend_common_base::rangemap::RangeMerger;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_metrics::storage::metrics_inc_copy_read_size_bytes;
use opendal::BlockingOperator;

use crate::parquet2::parquet_reader::MergeIOReadResult;
use crate::parquet2::parquet_reader::OwnerMemory;
use crate::parquet2::parquet_reader::Parquet2Reader;
use crate::parquet2::Parquet2RowGroupPart;
use crate::ReadSettings;

impl Parquet2Reader {
    pub fn sync_merge_io_read(
        read_settings: &ReadSettings,
        op: BlockingOperator,
        location: &str,
        raw_ranges: Vec<(usize, Range<u64>)>,
    ) -> Result<MergeIOReadResult> {
        let path = location.to_string();

        // Build merged read ranges.
        let ranges = raw_ranges
            .iter()
            .map(|(_, r)| r.clone())
            .collect::<Vec<_>>();
        let range_merger = RangeMerger::from_iter(
            ranges,
            read_settings.max_gap_size,
            read_settings.max_range_size,
        );
        let merged_ranges = range_merger.ranges();

        // Read merged range data.
        let mut io_res = Vec::with_capacity(merged_ranges.len());
        for (idx, range) in merged_ranges.iter().enumerate() {
            io_res.push(Self::sync_read_range(
                op.clone(),
                location,
                idx,
                range.start,
                range.end,
            )?);
        }

        let owner_memory = OwnerMemory::create(io_res);

        let mut read_res = MergeIOReadResult::create(owner_memory, raw_ranges.len(), path.clone());

        for (raw_idx, raw_range) in &raw_ranges {
            let column_id = *raw_idx;
            let column_range = raw_range.start..raw_range.end;

            // Find the range index and Range from merged ranges.
            let (merged_range_idx, merged_range) = range_merger.get(column_range.clone()).ok_or(ErrorCode::Internal(format!(
                "It's a terrible bug, not found raw range:[{:?}], path:{} from merged ranges\n: {:?}",
                column_range, path, merged_ranges
            )))?;

            // Fetch the raw data for the raw range.
            let start = (column_range.start - merged_range.start) as usize;
            let end = (column_range.end - merged_range.start) as usize;
            read_res.add_column_chunk(merged_range_idx, column_id, start..end);
        }

        Ok(read_res)
    }

    pub fn sync_read_columns_data_by_merge_io(
        &self,
        setting: &ReadSettings,
        part: &Parquet2RowGroupPart,
        operator: &BlockingOperator,
    ) -> Result<MergeIOReadResult> {
        let mut ranges = vec![];
        for index in self.columns_to_read() {
            let meta = &part.column_metas[index];
            ranges.push((*index, meta.offset..(meta.offset + meta.length)));
            metrics_inc_copy_read_size_bytes(meta.length);
        }
        Self::sync_merge_io_read(setting, operator.clone(), &part.location, ranges)
    }

    #[inline]
    pub fn sync_read_range(
        op: BlockingOperator,
        path: &str,
        index: usize,
        start: u64,
        end: u64,
    ) -> Result<(usize, Vec<u8>)> {
        let chunk = op.read_with(path).range(start..end).call()?;
        Ok((index, chunk))
    }
}
