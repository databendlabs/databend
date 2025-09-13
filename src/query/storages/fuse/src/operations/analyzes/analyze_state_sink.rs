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

use std::any::Any;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;
use std::time::Instant;

use async_channel::Receiver;
use async_channel::Sender;
use async_trait::async_trait;
use databend_common_base::base::tokio::sync::Semaphore;
use databend_common_base::runtime::GlobalIORuntime;
use databend_common_base::runtime::TrySpawn;
use databend_common_catalog::plan::PartInfoPtr;
use databend_common_catalog::plan::Projection;
use databend_common_catalog::table::Table;
use databend_common_catalog::table::TableExt;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::local_block_meta_serde;
use databend_common_expression::BlockMetaInfo;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::FieldIndex;
use databend_common_expression::TableField;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_core::Pipeline;
use databend_common_pipeline_sinks::AsyncSink;
use databend_common_pipeline_sinks::AsyncSinker;
use databend_common_storage::Datum;
use databend_common_storage::Histogram;
use databend_common_storage::HistogramBucket;
use databend_common_storage::MetaHLL;
use databend_storages_common_cache::BlockMeta;
use databend_storages_common_cache::CompactSegmentInfo;
use databend_storages_common_cache::LoadParams;
use databend_storages_common_cache::SegmentStatistics;
use databend_storages_common_index::RangeIndex;
use databend_storages_common_io::ReadSettings;
use databend_storages_common_table_meta::meta::decode_column_hll;
use databend_storages_common_table_meta::meta::encode_column_hll;
use databend_storages_common_table_meta::meta::AdditionalStatsMeta;
use databend_storages_common_table_meta::meta::BlockHLL;
use databend_storages_common_table_meta::meta::ClusterStatistics;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::RawBlockHLL;
use databend_storages_common_table_meta::meta::SegmentInfo;
use databend_storages_common_table_meta::meta::SnapshotId;
use databend_storages_common_table_meta::meta::Statistics;
use databend_storages_common_table_meta::meta::StatisticsOfColumns;
use databend_storages_common_table_meta::meta::TableSnapshot;
use databend_storages_common_table_meta::meta::TableSnapshotStatistics;
use databend_storages_common_table_meta::meta::Versioned;
use opendal::Operator;

use crate::io::build_column_hlls;
use crate::io::read::meta::SegmentStatsReader;
use crate::io::BlockReader;
use crate::io::CachedMetaWriter;
use crate::io::CompactSegmentInfoReader;
use crate::io::MetaReaders;
use crate::io::SegmentsIO;
use crate::io::TableMetaLocationGenerator;
use crate::operations::acquire_task_permit;
use crate::operations::analyzes::AnalyzeExtraMeta;
use crate::statistics::reduce_block_statistics;
use crate::statistics::reduce_cluster_statistics;
use crate::FuseLazyPartInfo;
use crate::FuseStorageFormat;
use crate::FuseTable;

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq)]
enum AnalyzeStep {
    CollectNDV,
    CollectHistogram,
    CommitStatistics,
}

struct SinkAnalyzeState {
    ctx: Arc<dyn TableContext>,
    input_port: Arc<InputPort>,

    table: Arc<dyn Table>,
    snapshot_id: SnapshotId,
    histogram_info_receivers: HashMap<u32, Receiver<DataBlock>>,
    input_data: Option<DataBlock>,
    committed: bool,
    row_count: u64,
    ndv_states: HashMap<ColumnId, MetaHLL>,
    histograms: HashMap<ColumnId, Histogram>,
    step: AnalyzeStep,
}

impl SinkAnalyzeState {
    #[allow(clippy::too_many_arguments)]
    pub fn create(
        ctx: Arc<dyn TableContext>,
        table: &FuseTable,
        snapshot_id: SnapshotId,
        input_port: Arc<InputPort>,
        histogram_info_receivers: HashMap<u32, Receiver<DataBlock>>,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Box::new(SinkAnalyzeState {
            ctx,
            input_port,
            table: Arc::new(table.clone()),
            snapshot_id,
            histogram_info_receivers,
            input_data: None,
            committed: false,
            row_count: 0,
            ndv_states: Default::default(),
            histograms: Default::default(),
            step: AnalyzeStep::CollectNDV,
        })))
    }

    #[async_backtrace::framed]
    async fn create_histogram(&mut self, col_id: u32, data_block: DataBlock) -> Result<()> {
        if data_block.num_rows() == 0 {
            return Ok(());
        }
        for row in 0..data_block.num_rows() {
            let column = &data_block.columns()[1];
            let value = column.index(row).clone().unwrap();
            let number = value.as_number().unwrap();
            let ndv = number.as_u_int64().unwrap();
            let upper_bound =
                Datum::from_scalar(data_block.get_by_offset(2).index(row).unwrap().to_owned())
                    .ok_or_else(|| {
                        ErrorCode::Internal("Don't support the type to generate histogram")
                    })?;
            let lower_bound =
                Datum::from_scalar(data_block.get_by_offset(3).index(row).unwrap().to_owned())
                    .ok_or_else(|| {
                        ErrorCode::Internal("Don't support the type to generate histogram")
                    })?;

            let count: Option<_> = try {
                *data_block
                    .get_by_offset(4)
                    .index(row)?
                    .as_number()?
                    .as_u_int64()?
            };

            let bucket =
                HistogramBucket::new(lower_bound, upper_bound, count.unwrap() as f64, *ndv as f64);
            self.histograms
                .entry(col_id)
                .and_modify(|histogram| histogram.add_bucket(bucket.clone()))
                .or_insert(Histogram::new(vec![bucket], true));
        }
        Ok(())
    }

    async fn commit_statistics(&mut self) -> Result<()> {
        let table = self.table.refresh(self.ctx.as_ref()).await?;
        let table = FuseTable::try_from_table(table.as_ref())?;
        let snapshot = table.read_table_snapshot().await?;
        if snapshot.is_none() {
            return Ok(());
        }
        let snapshot = snapshot.unwrap();
        let column_ids = snapshot.schema.to_leaf_column_id_set();
        self.ndv_states.retain(|k, _| column_ids.contains(k));

        let mut new_snapshot = TableSnapshot::try_from_previous(
            snapshot.clone(),
            Some(table.get_table_info().ident.seq),
            self.ctx
                .get_table_meta_timestamps(table, Some(snapshot.clone()))?,
        )?;

        // 3. Generate new table statistics
        new_snapshot.summary.additional_stats_meta = Some(AdditionalStatsMeta {
            hll: Some(encode_column_hll(&self.ndv_states)?),
            row_count: snapshot.summary.row_count,
            ..Default::default()
        });

        let table_statistics = if self.ctx.get_settings().get_enable_table_snapshot_stats()? {
            let stats = TableSnapshotStatistics::new(
                self.ndv_states.clone(),
                self.histograms.clone(),
                self.snapshot_id,
                snapshot.summary.row_count,
            );
            new_snapshot.table_statistics_location = Some(
                table
                    .meta_location_generator
                    .snapshot_statistics_location_from_uuid(
                        &stats.snapshot_id,
                        stats.format_version(),
                    )?,
            );
            Some(stats)
        } else {
            None
        };

        let (col_stats, cluster_stats) =
            self.regenerate_statistics(table, snapshot.as_ref()).await?;
        // 4. Save table statistics
        new_snapshot.summary.col_stats = col_stats;
        new_snapshot.summary.cluster_stats = cluster_stats;
        table
            .commit_to_meta_server(
                self.ctx.as_ref(),
                &table.table_info,
                &table.meta_location_generator,
                new_snapshot,
                table_statistics,
                &None,
                &table.operator,
            )
            .await
    }

    async fn regenerate_statistics(
        &self,
        table: &FuseTable,
        snapshot: &TableSnapshot,
    ) -> Result<(StatisticsOfColumns, Option<ClusterStatistics>)> {
        // 1. Read table snapshot.
        let default_cluster_key_id = table.cluster_key_id();

        // 2. Iterator segments and blocks to estimate statistics.
        let mut read_segment_count = 0;
        let mut col_stats = HashMap::new();
        let mut cluster_stats = None;

        let start = Instant::now();
        let segments_io =
            SegmentsIO::create(self.ctx.clone(), table.operator.clone(), table.schema());
        let chunk_size = self.ctx.get_settings().get_max_threads()? as usize * 4;
        let number_segments = snapshot.segments.len();
        for chunk in snapshot.segments.chunks(chunk_size) {
            let mut stats_of_columns = Vec::new();
            let mut blocks_cluster_stats = Vec::new();
            if !col_stats.is_empty() {
                stats_of_columns.push(col_stats.clone());
                blocks_cluster_stats.push(cluster_stats.clone());
            }

            let segments = segments_io
                .read_segments::<SegmentInfo>(chunk, true)
                .await?;
            for segment in segments {
                let segment = segment?;
                stats_of_columns.push(segment.summary.col_stats.clone());
                blocks_cluster_stats.push(segment.summary.cluster_stats.clone());
            }

            // Generate new column statistics for snapshot
            col_stats = reduce_block_statistics(&stats_of_columns);
            cluster_stats =
                reduce_cluster_statistics(&blocks_cluster_stats, default_cluster_key_id);

            // Status.
            {
                read_segment_count += chunk.len();
                let status = format!(
                    "analyze: read segment files:{}/{}, cost:{:?}",
                    read_segment_count,
                    number_segments,
                    start.elapsed()
                );
                self.ctx.set_status_info(&status);
            }
        }

        for (id, hll) in &self.ndv_states {
            if let Some(stats) = col_stats.get_mut(id) {
                let ndv = hll.count() as u64;
                stats.distinct_of_values = Some(ndv);
            }
        }
        Ok((col_stats, cluster_stats))
    }
}

#[async_trait::async_trait]
impl Processor for SinkAnalyzeState {
    fn name(&self) -> String {
        "SinkAnalyzeState".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.input_data.is_some() {
            if !self.input_port.has_data() {
                self.input_port.set_need_data();
            }
            return Ok(Event::Sync);
        }

        if self.input_port.is_finished() {
            match self.step {
                AnalyzeStep::CollectNDV => {
                    self.step = AnalyzeStep::CollectHistogram;
                    return Ok(Event::Async);
                }
                AnalyzeStep::CollectHistogram => {
                    return Ok(Event::Async);
                }
                AnalyzeStep::CommitStatistics => {
                    if self.committed {
                        return Ok(Event::Finished);
                    } else {
                        return Ok(Event::Async);
                    }
                }
            }
        }

        if self.input_port.has_data() {
            self.input_data = Some(self.input_port.pull_data().unwrap()?);
            return Ok(Event::Sync);
        }

        self.input_port.set_need_data();
        Ok(Event::NeedData)
    }

    fn process(&mut self) -> Result<()> {
        match self.step {
            AnalyzeStep::CollectNDV => {
                if let Some(mut data_block) = self.input_data.take() {
                    assert!(data_block.is_empty());
                    if let Some(meta) = data_block
                        .take_meta()
                        .and_then(AnalyzeExtraMeta::downcast_from)
                    {
                        if meta.row_count > 0 {
                            for (column_id, column_hll) in meta.column_hlls.iter() {
                                self.ndv_states
                                    .entry(*column_id)
                                    .and_modify(|hll| hll.merge(column_hll))
                                    .or_insert_with(|| column_hll.clone());
                            }
                            self.row_count += meta.row_count;
                        }
                    }
                }
            }
            _ => unreachable!(),
        }
        Ok(())
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        match self.step {
            AnalyzeStep::CollectHistogram => {
                let mut finished_count = 0;
                let receivers = self.histogram_info_receivers.clone();
                for (id, receiver) in receivers.iter() {
                    if let Ok(res) = receiver.recv().await {
                        self.create_histogram(*id, res).await?;
                    } else {
                        finished_count += 1;
                    }
                }
                if finished_count == self.histogram_info_receivers.len() {
                    self.step = AnalyzeStep::CommitStatistics;
                }
            }
            AnalyzeStep::CommitStatistics => {
                let mismatch_code = ErrorCode::TABLE_VERSION_MISMATCHED;
                loop {
                    if let Err(e) = self.commit_statistics().await {
                        if e.code() == mismatch_code {
                            log::warn!("Retry after got TableVersionMismatched");
                            continue;
                        } else {
                            return Err(e);
                        }
                    }
                    break;
                }
                self.committed = true;
            }
            _ => unreachable!(),
        }
        Ok(())
    }
}

pub struct HistogramInfoSink {
    sender: Option<Sender<DataBlock>>,
}

impl HistogramInfoSink {
    pub fn create(tx: Option<Sender<DataBlock>>, input: Arc<InputPort>) -> Box<dyn Processor> {
        AsyncSinker::create(input, HistogramInfoSink { sender: tx })
    }
}

#[async_trait]
impl AsyncSink for HistogramInfoSink {
    const NAME: &'static str = "HistogramInfoSink";

    #[async_backtrace::framed]
    async fn on_finish(&mut self) -> Result<()> {
        drop(self.sender.take());
        Ok(())
    }

    #[async_backtrace::framed]
    async fn consume(&mut self, data_block: DataBlock) -> Result<bool> {
        if let Some(sender) = self.sender.as_ref() {
            if sender.send(data_block).await.is_err() {
                return Err(ErrorCode::Internal("HistogramInfoSink sender failed"));
            };
        }
        Ok(false)
    }
}
