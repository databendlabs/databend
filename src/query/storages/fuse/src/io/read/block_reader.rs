// Copyright 2021 Datafuse Labs.
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
use std::time::Instant;

use common_arrow::parquet::metadata::SchemaDescriptor;
use common_base::rangemap::RangeMerger;
use common_base::runtime::UnlimitedFuture;
use common_catalog::plan::Projection;
use common_expression::TableSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_storage::ColumnLeaves;
use opendal::Object;
use opendal::Operator;

use crate::metrics::metrics_inc_remote_io_copy_milliseconds;
use crate::metrics::metrics_inc_remote_io_read_bytes_after_merged;
use crate::metrics::metrics_inc_remote_io_read_milliseconds;
use crate::metrics::metrics_inc_remote_io_seeks_after_merged;

// TODO: make BlockReader as a trait.
#[derive(Clone)]
pub struct BlockReader {
    pub(crate) operator: Operator,
    pub(crate) projection: Projection,
    pub(crate) projected_schema: TableSchemaRef,
    pub(crate) column_leaves: ColumnLeaves,
    pub(crate) parquet_schema_descriptor: SchemaDescriptor,
}

impl BlockReader {
    /// This is an optimized for data read, works like the Linux kernel io-scheduler IO merging.
    /// If the distance between two IO request ranges to be read is less than storage_io_min_bytes_for_seek(Default is 48Bytes),
    /// will read the range that contains both ranges, thus avoiding extra seek.
    ///
    /// It will *NOT* merge two requests:
    /// if the last io request size is larger than storage_io_page_bytes_for_read(Default is 512KB).
    pub async fn merge_io_read(
        object: Object,
        min_seek_bytes: u64,
        max_page_bytes: u64,
        raw_ranges: Vec<(usize, Range<u64>)>,
    ) -> Result<Vec<(usize, Vec<u8>)>> {
        let path = object.path().to_string();

        // Build merged read ranges.
        let ranges = raw_ranges
            .iter()
            .map(|(_, r)| r.clone())
            .collect::<Vec<_>>();
        let range_merger = RangeMerger::from_iter(ranges, min_seek_bytes, max_page_bytes);
        let merged_ranges = range_merger.ranges();

        // Read merged range data.
        let mut read_handlers = Vec::with_capacity(merged_ranges.len());
        for (idx, range) in merged_ranges.iter().enumerate() {
            // Perf.
            {
                metrics_inc_remote_io_seeks_after_merged(1);
                metrics_inc_remote_io_read_bytes_after_merged(range.end - range.start);
            }

            read_handlers.push(UnlimitedFuture::create(Self::read_range(
                object.clone(),
                idx,
                range.start,
                range.end,
            )));
        }

        let start = Instant::now();
        let merged_range_data_results = futures::future::try_join_all(read_handlers).await?;

        // Perf.
        metrics_inc_remote_io_read_milliseconds(start.elapsed().as_millis() as u64);

        // Build raw range data from merged range data.
        let mut final_result = Vec::with_capacity(raw_ranges.len());

        let start = Instant::now();
        for (raw_idx, raw_range) in &raw_ranges {
            let column_start = raw_range.start;
            let column_end = raw_range.end;

            // Find the range index and Range from merged ranges.
            let (merged_range_idx, merged_range) = match range_merger.get(column_start..column_end)
            {
                None => Err(ErrorCode::Internal(format!(
                    "It's a terrible bug, not found raw range:[{},{}], path:{} from merged ranges\n: {:?}",
                    column_start, column_end, path, merged_ranges
                ))),
                Some((i, r)) => Ok((i, r)),
            }?;

            // Find the range data by the merged index.
            let mut merged_range_data = None;
            for (i, data) in &merged_range_data_results {
                if *i == merged_range_idx {
                    merged_range_data = Some(data);
                    break;
                }
            }
            let merged_range_data = match merged_range_data {
                None => Err(ErrorCode::Internal(format!(
                    "It's a terrible bug, not found range data, merged_range_idx:{}, path:{}",
                    merged_range_idx, path
                ))),
                Some(v) => Ok(v),
            }?;

            // Fetch the raw data for the raw range.
            let start = (column_start - merged_range.start) as usize;
            let end = (column_end - merged_range.start) as usize;
            // Here is a heavy copy.
            let column_data = merged_range_data[start..end].to_vec();
            final_result.push((*raw_idx, column_data));
        }
        // Perf.
        {
            metrics_inc_remote_io_copy_milliseconds(start.elapsed().as_millis() as u64);
        }

        Ok(final_result)
    }

    #[inline]
    pub async fn read_range(
        o: Object,
        index: usize,
        start: u64,
        end: u64,
    ) -> Result<(usize, Vec<u8>)> {
        let chunk = o.range_read(start..end).await?;
        Ok((index, chunk))
    }
}
