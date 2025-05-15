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

use std::collections::HashSet;
use std::sync::Arc;

use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;

use super::table::ParquetTable;
use crate::parquet_part::collect_file_parts;
use crate::parquet_part::collect_small_file_parts;
use crate::ParquetPart;

impl ParquetTable {
    #[inline]
    #[async_backtrace::framed]
    pub(super) async fn do_read_partitions(
        &self,
        ctx: Arc<dyn TableContext>,
        push_down: Option<PushDownInfo>,
    ) -> Result<(PartStatistics, Partitions)> {
        let thread_num = ctx.get_settings().get_max_threads()? as usize;

        let file_locations = {
            match &self.files_to_read {
                Some(files) => files
                    .iter()
                    .filter(|f| f.size > 0)
                    .map(|f| (f.path.clone(), f.size, f.dedup_key()))
                    .collect::<Vec<_>>(),
                None => self
                    .files_info
                    .list(&self.operator, thread_num, None)
                    .await?
                    .into_iter()
                    .filter(|f| f.size > 0)
                    .map(|f| (f.path.clone(), f.size, f.dedup_key()))
                    .collect::<Vec<_>>(),
            }
        };

        // It will be used to calculate the memory will be used in reading.
        let columns_to_read = if let Some(prewhere) =
            PushDownInfo::prewhere_of_push_downs(push_down.as_ref())
        {
            let (_, prewhere_columns) = prewhere
                .prewhere_columns
                .to_arrow_projection(&self.schema_descr);
            let (_, output_columns) = prewhere
                .output_columns
                .to_arrow_projection(&self.schema_descr);
            let mut columns = HashSet::with_capacity(prewhere_columns.len() + output_columns.len());
            columns.extend(prewhere_columns);
            columns.extend(output_columns);
            let mut columns = columns.into_iter().collect::<Vec<_>>();
            columns.sort();
            columns
        } else {
            let output_projection =
                PushDownInfo::projection_of_push_downs(&self.schema(), push_down.as_ref());
            let (_, columns) = output_projection.to_arrow_projection(&self.schema_descr);
            columns
        };
        let num_columns_to_read = columns_to_read.len();

        let mut partitions = Partitions::default();
        let mut stats = PartStatistics::default();

        let fast_read_bytes = ctx.get_settings().get_parquet_fast_read_bytes()?;
        let mut large_files = vec![];
        let mut small_files = vec![];
        for (location, size, dedup_key) in file_locations.into_iter() {
            if size > fast_read_bytes {
                large_files.push((location, size, dedup_key));
            } else if size > 0 {
                small_files.push((location, size, dedup_key));
            }
        }

        collect_file_parts(
            large_files,
            self.compression_ratio,
            &mut partitions,
            &mut stats,
            num_columns_to_read,
            self.schema().num_fields(),
        );

        if !small_files.is_empty() {
            let mut max_compression_ratio = self.compression_ratio;
            let mut max_compressed_size = 0u64;
            for part in partitions.partitions.iter() {
                let p = part.as_any().downcast_ref::<ParquetPart>().unwrap();
                max_compression_ratio = max_compression_ratio
                    .max(p.uncompressed_size() as f64 / p.compressed_size() as f64);
                max_compressed_size = max_compressed_size.max(p.compressed_size());
            }

            collect_small_file_parts(
                small_files,
                max_compression_ratio,
                max_compressed_size,
                &mut partitions,
                &mut stats,
            );
        }

        Ok((stats, partitions))
    }
}
