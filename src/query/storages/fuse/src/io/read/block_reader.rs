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
use std::sync::Arc;

use common_arrow::parquet::metadata::SchemaDescriptor;
use common_base::rangemap::RangeMerger;
use common_base::runtime::UnlimitedFuture;
use common_catalog::plan::Projection;
use common_catalog::table_context::TableContext;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_storage::ColumnLeaves;
use opendal::Object;
use opendal::Operator;

// TODO: make BlockReader as a trait.
#[derive(Clone)]
pub struct BlockReader {
    pub(crate) operator: Operator,
    pub(crate) projection: Projection,
    pub(crate) projected_schema: DataSchemaRef,
    pub(crate) column_leaves: ColumnLeaves,
    pub(crate) parquet_schema_descriptor: SchemaDescriptor,
}

impl BlockReader {
    /// This is an optimized for data read, works like the Linux kernel io-scheduler IO merging.
    /// It will merge two requests:
    /// if the gap is less than get_storage_io_min_bytes_for_seek (Default is 512Bytes).
    ///
    /// It will *NOT* merge two requests:
    /// if the last io request size is larger than get_storage_io_page_bytes_for_read(Default is 512KB).
    pub async fn merge_io_read(
        ctx: Arc<dyn TableContext>,
        object: Object,
        raw_ranges: Vec<(usize, Range<u64>)>,
    ) -> Result<Vec<(usize, Vec<u8>)>> {
        // Merge settings.
        let min_bytes_for_seek = ctx.get_settings().get_storage_io_min_bytes_for_seek()?;
        let max_page_bytes_for_read = ctx
            .get_settings()
            .get_storage_io_max_page_bytes_for_read()?;

        // Build merged read ranges.
        let ranges = raw_ranges
            .iter()
            .map(|(_, r)| r.clone())
            .collect::<Vec<_>>();
        let range_merger =
            RangeMerger::from_iter(ranges, min_bytes_for_seek, max_page_bytes_for_read);
        let merged_ranges = range_merger.ranges();

        // Read merged range data.
        let mut read_handlers = Vec::with_capacity(merged_ranges.len());
        for (idx, range) in merged_ranges.iter().enumerate() {
            let obj = object.clone();

            // Push the fut to tokio scheduler queue.
            read_handlers.push(async move {
                let path = obj.path().to_string();
                let handler = common_base::base::tokio::spawn(UnlimitedFuture::create(
                    Self::read_range(obj, idx, range.start, range.end),
                ));
                handler.await.map_err(|e| {
                    ErrorCode::StorageOther(format!(
                        "merge io read range:[{},{}], index:{}, path:{}, error: {}",
                        range.start, range.end, idx, path, e
                    ))
                })?
            });
        }
        let merged_range_data_results = futures::future::try_join_all(read_handlers).await?;

        // Build raw range data from merged range data.
        let mut final_result = Vec::with_capacity(raw_ranges.len());
        for (raw_idx, raw_range) in &raw_ranges {
            let column_start = raw_range.start;
            let column_end = raw_range.end;

            // Find the range index and Range from merged ranges.
            let (merged_range_idx, merged_range) = match range_merger.get(column_start..column_end)
            {
                None => Err(ErrorCode::Internal(format!(
                    "It's a terrible bug, not found raw range:[{},{}] from merged ranges\n: {:?}",
                    column_start, column_end, merged_ranges
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
                    "It's a terrible bug, not found range data, merged_range_idx:{}",
                    merged_range_idx
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
