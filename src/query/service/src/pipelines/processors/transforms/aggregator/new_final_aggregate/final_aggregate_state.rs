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
use std::time::Instant;

use databend_common_base::base::ProgressValues;
use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::arrow::deserialize_column;
use databend_common_expression::arrow::serialize_column;
use databend_common_expression::DataBlock;
use databend_common_pipeline_transforms::MemorySettings;
use log::info;
use opendal::BlockingOperator;
use opendal::Operator;

use crate::pipelines::memory_settings::MemorySettingsExt;
use crate::pipelines::processors::transforms::aggregator::AggregateMeta;
use crate::pipelines::processors::transforms::aggregator::BucketSpilledPayload;
use crate::pipelines::processors::transforms::aggregator::SerializedPayload;
use crate::sessions::QueryContext;
use crate::spillers::Spiller;
use crate::spillers::SpillerConfig;
use crate::spillers::SpillerType;
pub struct FinalAggregateSharedState {
    pub aggregate_queues: Vec<Vec<AggregateMeta>>,
}

impl FinalAggregateSharedState {
    pub fn create(partition_count: usize) -> Self {
        let mut aggregate_queues = Vec::with_capacity(partition_count);
        for _ in 0..partition_count {
            aggregate_queues.push(Vec::new());
        }

        FinalAggregateSharedState { aggregate_queues }
    }

    pub fn merge_aggregate_queues(&mut self, metas: Vec<Vec<AggregateMeta>>) {
        debug_assert_eq!(self.aggregate_queues.len(), metas.len());
        for (i, meta_queue) in metas.into_iter().enumerate() {
            self.aggregate_queues[i].extend(meta_queue);
        }
    }

    pub fn take_aggregate_queue(&mut self, index: usize) -> Vec<AggregateMeta> {
        std::mem::take(&mut self.aggregate_queues[index])
    }
}

pub struct FinalAggregateSpiller {
    pub spiller: Spiller,
    pub blocking_operator: BlockingOperator,
    pub memory_settings: MemorySettings,
    pub ctx: Arc<QueryContext>,
    pub is_spilled: bool,
}

impl FinalAggregateSpiller {
    pub fn try_create(ctx: Arc<QueryContext>, operator: Operator) -> Result<Self> {
        let memory_settings = MemorySettings::from_aggregate_settings(&ctx)?;

        let location_prefix = ctx.query_id_spill_prefix();

        let config = SpillerConfig {
            spiller_type: SpillerType::Aggregation,
            location_prefix,
            disk_spill: None,
            use_parquet: ctx.get_settings().get_spilling_file_format()?.is_parquet(),
        };
        let spiller = Spiller::create(ctx.clone(), operator.clone(), config)?;

        let blocking_operator = operator.blocking();

        Ok(Self {
            spiller,
            blocking_operator,
            memory_settings,
            ctx,
            is_spilled: false,
        })
    }

    pub fn restore(&self, payload: BucketSpilledPayload) -> Result<AggregateMeta> {
        // read
        let instant = Instant::now();
        let data = self
            .blocking_operator
            .read_with(&payload.location)
            .range(payload.data_range.clone())
            .call()?
            .to_vec();

        self.record_read_profile(&instant, data.len());

        // deserialize
        let mut begin = 0;
        let mut columns = Vec::with_capacity(payload.columns_layout.len());
        for &column_layout in &payload.columns_layout {
            columns.push(deserialize_column(
                &data[begin..begin + column_layout as usize],
            )?);
            begin += column_layout as usize;
        }

        Ok(AggregateMeta::Serialized(SerializedPayload {
            bucket: payload.bucket,
            data_block: DataBlock::new_from_columns(columns),
            max_partition_count: payload.max_partition_count,
        }))
    }

    pub fn spill(&self, id: usize, data_block: DataBlock) -> Result<BucketSpilledPayload> {
        let rows = data_block.num_rows();
        let mut columns_layout = Vec::with_capacity(data_block.num_columns());
        let mut columns_data = Vec::with_capacity(data_block.num_columns());

        for entry in data_block.columns() {
            let column = entry.as_column().ok_or_else(|| {
                ErrorCode::Internal("Unexpected scalar when spilling aggregate data")
            })?;
            let column_data = serialize_column(column);
            columns_layout.push(column_data.len() as u64);
            columns_data.push(column_data);
        }

        let location = self.spiller.create_unique_location();

        let instant = Instant::now();

        let mut writer = self
            .blocking_operator
            .writer_with(&location)
            .chunk(8 * 1024 * 1024)
            .call()?;

        let mut write_bytes = 0;
        for data in columns_data.into_iter() {
            write_bytes += data.len();
            writer.write(data)?;
        }

        writer.close()?;

        self.spiller
            .add_aggregate_spill_file(&location, write_bytes);

        Profile::record_usize_profile(ProfileStatisticsName::RemoteSpillWriteCount, 1);
        Profile::record_usize_profile(ProfileStatisticsName::RemoteSpillWriteBytes, write_bytes);
        Profile::record_usize_profile(
            ProfileStatisticsName::RemoteSpillWriteTime,
            instant.elapsed().as_millis() as usize,
        );

        let progress_val = ProgressValues {
            rows,
            bytes: write_bytes,
        };
        self.ctx.get_aggregate_spill_progress().incr(&progress_val);

        info!(
            "Write aggregate spill {} successfully, elapsed: {:?}",
            location,
            instant.elapsed()
        );

        let payload = BucketSpilledPayload {
            bucket: id as isize,
            location,
            data_range: 0..write_bytes as u64,
            columns_layout,
            max_partition_count: 0,
        };

        Ok(payload)
    }

    fn record_read_profile(&self, instant: &Instant, read_bytes: usize) {
        Profile::record_usize_profile(ProfileStatisticsName::RemoteSpillReadCount, 1);
        Profile::record_usize_profile(ProfileStatisticsName::RemoteSpillReadBytes, read_bytes);
        Profile::record_usize_profile(
            ProfileStatisticsName::RemoteSpillReadTime,
            instant.elapsed().as_millis() as usize,
        );
    }
}
