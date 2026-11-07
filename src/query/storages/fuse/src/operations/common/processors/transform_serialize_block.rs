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
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use databend_common_base::base::ProgressValues;
use databend_common_catalog::plan::VirtualColumnLayout;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::ComputedExpr;
use databend_common_expression::DataBlock;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRef;
use databend_common_metrics::storage::metrics_inc_block_write_milliseconds;
use databend_common_metrics::storage::metrics_inc_block_write_nums;
use databend_common_metrics::storage::metrics_inc_recluster_write_block_nums;
use databend_common_pipeline::core::Event;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::PipeItem;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_sql::executor::physical_plans::MutationKind;
use databend_common_storage::MutationStatus;
use databend_storages_common_blocks::ColumnWiseParquetWriter;
use databend_storages_common_index::BloomIndex;
use databend_storages_common_index::RangeIndex;
use databend_storages_common_table_meta::meta::ExtendedBlockMeta;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;
use opendal::Operator;
use parquet::file::metadata::ParquetMetaData;

use super::recluster_inverted_index_merge::ReclusterIndexInput;
use super::recluster_inverted_index_merge::ReclusterIndexOutput;
use super::recluster_inverted_index_merge::ReclusterIndexRowRange;
use crate::FuseStorageFormat;
use crate::FuseTable;
use crate::io::BlockBuilder;
use crate::io::BlockSerialization;
use crate::io::BlockWriter;
use crate::io::JsonPathStatisticsBuilder;
use crate::io::VirtualColumnBuilder;
use crate::io::block_index::create_block_index_specs;
use crate::operations::column_parquet_metas;
use crate::operations::common::BlockMetaIndex;
use crate::operations::common::MutationLogEntry;
use crate::operations::common::MutationLogs;
use crate::operations::mutation::ClusterStatsGenType;
use crate::operations::mutation::SerializeDataMeta;
use crate::statistics::ClusterStatsGenerator;

#[allow(clippy::large_enum_variant)]
enum State {
    Consume,
    NeedSerialize {
        block: DataBlock,
        stats_type: ClusterStatsGenType,
        index: Option<BlockMetaIndex>,
        virtual_column_layout: Option<VirtualColumnLayout>,
    },
    Serialized {
        serialized: BlockSerialization,
        index: Option<BlockMetaIndex>,
    },
    /// Column-wise path: encode the next column (sync).
    EncodeColumn(Box<PendingColumnWise>),
    /// Column-wise path: upload the encoded bytes of one column, or the footer (async).
    UploadColumn {
        pending: Box<PendingColumnWise>,
        chunks: Vec<Bytes>,
    },
}

/// In-flight state of a block being encoded and uploaded one column at a time.
struct PendingColumnWise {
    serialized: BlockSerialization,
    index: Option<BlockMetaIndex>,
    /// `None` once the footer has been produced.
    parquet: Option<ColumnWiseParquetWriter>,
    schema: TableSchemaRef,
    writer: Option<opendal::Writer>,
    /// Set together with the footer chunks; its presence marks the final upload.
    metadata: Option<ParquetMetaData>,
    written: u64,
    start: Instant,
}

pub struct TransformSerializeBlock {
    state: State,
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    output_data: Option<DataBlock>,

    block_builder: BlockBuilder,
    dal: Operator,
    table_id: Option<u64>, // Only used in multi table insert
    kind: MutationKind,
    pending_merge_hll: bool,
    pending_logical_change: (u64, u64),
    /// Encode parquet column by column and upload each column via multipart upload.
    column_wise_upload: bool,
    recluster_index_rows: Option<Vec<ReclusterIndexRowRange>>,
    recluster_output_row: u64,
    recluster_merged_names: Vec<String>,
}

impl TransformSerializeBlock {
    pub fn try_create(
        ctx: Arc<dyn TableContext>,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        table: &FuseTable,
        cluster_stats_gen: ClusterStatsGenerator,
        kind: MutationKind,
        table_meta_timestamps: TableMetaTimestamps,
    ) -> Result<Self> {
        Self::do_create(
            ctx,
            input,
            output,
            table,
            cluster_stats_gen,
            kind,
            false,
            None,
            table_meta_timestamps,
        )
    }

    pub fn try_create_with_tid(
        ctx: Arc<dyn TableContext>,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        table: &FuseTable,
        cluster_stats_gen: ClusterStatsGenerator,
        kind: MutationKind,
        table_meta_timestamps: TableMetaTimestamps,
    ) -> Result<Self> {
        Self::do_create(
            ctx,
            input,
            output,
            table,
            cluster_stats_gen,
            kind,
            true,
            None,
            table_meta_timestamps,
        )
    }

    pub fn try_create_with_virtual_layout(
        ctx: Arc<dyn TableContext>,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        table: &FuseTable,
        cluster_stats_gen: ClusterStatsGenerator,
        kind: MutationKind,
        virtual_column_layout: Arc<VirtualColumnLayout>,
        table_meta_timestamps: TableMetaTimestamps,
    ) -> Result<Self> {
        Self::do_create(
            ctx,
            input,
            output,
            table,
            cluster_stats_gen,
            kind,
            false,
            Some(virtual_column_layout),
            table_meta_timestamps,
        )
    }

    fn do_create(
        ctx: Arc<dyn TableContext>,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        table: &FuseTable,
        cluster_stats_gen: ClusterStatsGenerator,
        kind: MutationKind,
        with_tid: bool,
        virtual_column_layout: Option<Arc<VirtualColumnLayout>>,
        table_meta_timestamps: TableMetaTimestamps,
    ) -> Result<Self> {
        let schema = table.schema();
        // remove virtual computed fields.
        let mut fields = schema
            .fields()
            .iter()
            .filter(|f| !matches!(f.computed_expr(), Some(ComputedExpr::Virtual(_))))
            .cloned()
            .collect::<Vec<_>>();
        if !matches!(kind, MutationKind::Insert | MutationKind::Replace) {
            // add stream fields.
            for stream_column in table.stream_columns().iter() {
                fields.push(stream_column.table_field());
            }
        }
        let source_schema = Arc::new(TableSchema {
            fields,
            ..schema.as_ref().clone()
        });

        let bloom_columns_map = table
            .bloom_index_cols
            .bloom_index_fields(source_schema.clone(), BloomIndex::supported_type)?;
        let ndv_columns_map = table
            .approx_distinct_cols
            .distinct_column_fields(source_schema.clone(), RangeIndex::supported_table_type)?;
        let top_n = if matches!(kind, MutationKind::Insert) {
            table.append_top_n_columns(source_schema.clone())?
        } else {
            None
        };
        let block_index_specs = create_block_index_specs(table, source_schema.clone())?;

        // Recluster/compact/refresh materialize virtual columns and reuse the
        // path frequencies collected by VirtualColumnBuilder. Other mutations
        // only collect JSON path statistics.
        let (virtual_column_builder, json_path_statistics_builder) =
            if table.enable_virtual_column() {
                match kind {
                    MutationKind::Recluster | MutationKind::Compact | MutationKind::Refresh => (
                        VirtualColumnBuilder::try_create(
                            source_schema.clone(),
                            table.virtual_column_layout_policy(),
                        )
                        .map(|builder| match &virtual_column_layout {
                            Some(layout) => builder.with_adaptive_layout(layout.clone()),
                            None => builder,
                        })
                        .ok(),
                        None,
                    ),
                    _ => (
                        None,
                        JsonPathStatisticsBuilder::try_create(
                            source_schema.clone(),
                            table.virtual_column_layout_policy(),
                        )
                        .ok(),
                    ),
                }
            } else {
                (None, None)
            };
        let serialize_hll = if matches!(
            kind,
            MutationKind::Insert
                | MutationKind::Replace
                | MutationKind::Update
                | MutationKind::MergeInto
        ) {
            // Merge blocks hll when insert, replace, update or merge into.
            false
        } else {
            true
        };

        let write_settings = table.get_write_settings();
        let column_wise_upload = ctx
            .get_settings()
            .get_enable_fuse_parquet_column_wise_upload()?
            && matches!(write_settings.storage_format, FuseStorageFormat::Parquet);
        let block_builder = BlockBuilder {
            ctx,
            operator: table.get_operator(),
            meta_locations: table.meta_location_generator().clone(),
            source_schema,
            write_settings,
            cluster_stats_gen,
            bloom_columns_map,
            ndv_columns_map,
            top_n,
            block_index_specs,
            virtual_column_builder,
            json_path_statistics_builder,
            table_meta_timestamps,
            serialize_hll,
        };
        Ok(TransformSerializeBlock {
            state: State::Consume,
            input,
            output,
            output_data: None,
            block_builder,
            dal: table.get_operator(),
            table_id: if with_tid { Some(table.get_id()) } else { None },
            kind,
            recluster_index_rows: None,
            recluster_output_row: 0,
            recluster_merged_names: Vec::new(),
            pending_merge_hll: false,
            pending_logical_change: (0, 0),
            column_wise_upload,
        })
    }

    pub fn into_processor(self) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Box::new(self)))
    }

    pub fn into_pipe_item(self) -> PipeItem {
        let input = self.input.clone();
        let output = self.output.clone();
        let processor_ptr = ProcessorPtr::create(Box::new(self));
        PipeItem::create(processor_ptr, vec![input], vec![output])
    }

    pub fn get_block_builder(&self) -> BlockBuilder {
        self.block_builder.clone()
    }

    fn mutation_logs(
        entry: MutationLogEntry,
        logical_updated_rows: u64,
        logical_deleted_rows: u64,
    ) -> DataBlock {
        let meta = MutationLogs {
            entries: vec![entry],
            logical_updated_rows,
            logical_deleted_rows,
        };
        DataBlock::empty_with_meta(Box::new(meta))
    }
}

#[async_trait::async_trait]
impl Processor for TransformSerializeBlock {
    fn name(&self) -> String {
        "TransformSerializeBlock".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if matches!(
            self.state,
            State::NeedSerialize { .. } | State::EncodeColumn(_)
        ) {
            return Ok(Event::Sync);
        }

        if matches!(
            self.state,
            State::Serialized { .. } | State::UploadColumn { .. }
        ) {
            return Ok(Event::Async);
        }

        if self.output.is_finished() {
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            return Ok(Event::NeedConsume);
        }

        if let Some(data_block) = self.output_data.take() {
            self.output.push_data(Ok(data_block));
            return Ok(Event::NeedConsume);
        }

        if self.input.is_finished() {
            self.output.finish();
            return Ok(Event::Finished);
        }

        if !self.input.has_data() {
            self.input.set_need_data();
            return Ok(Event::NeedData);
        }

        let mut input_data = self.input.pull_data().unwrap()?;
        let meta = input_data.take_meta();
        if let Some(meta) = meta {
            if ReclusterIndexInput::downcast_ref_from(&meta).is_some() {
                let input = ReclusterIndexInput::downcast_from(meta).unwrap();
                if !matches!(self.kind, MutationKind::Recluster) {
                    return Err(ErrorCode::Internal(
                        "index merge metadata outside recluster",
                    ));
                }
                self.recluster_output_row = input.output_row;
                self.recluster_index_rows = Some(input.rows);
                self.recluster_merged_names = input.merged_names;
                self.state = State::NeedSerialize {
                    block: input_data,
                    stats_type: ClusterStatsGenType::Generally,
                    index: None,
                    virtual_column_layout: None,
                };
                return Ok(Event::Sync);
            }
            let meta = SerializeDataMeta::downcast_from(meta)
                .ok_or_else(|| ErrorCode::Internal("It's a bug"))?;
            match meta {
                SerializeDataMeta::DeletedSegment(deleted_segment) => {
                    // delete a whole segment, segment level
                    let logical_deleted_rows = deleted_segment.summary.row_count;
                    let data_block = Self::mutation_logs(
                        MutationLogEntry::DeletedSegment { deleted_segment },
                        0,
                        logical_deleted_rows,
                    );
                    self.output.push_data(Ok(data_block));
                    Ok(Event::NeedConsume)
                }
                SerializeDataMeta::SerializeBlock(serialize_block) => {
                    if input_data.is_empty() {
                        // delete a whole block, block level
                        let data_block = Self::mutation_logs(
                            MutationLogEntry::DeletedBlock {
                                index: serialize_block.index,
                            },
                            serialize_block.logical_updated_rows,
                            serialize_block.logical_deleted_rows,
                        );
                        self.output.push_data(Ok(data_block));
                        Ok(Event::NeedConsume)
                    } else {
                        // replace the old block
                        self.pending_logical_change = (
                            serialize_block.logical_updated_rows,
                            serialize_block.logical_deleted_rows,
                        );
                        self.state = State::NeedSerialize {
                            block: input_data,
                            stats_type: serialize_block.stats_type,
                            index: Some(serialize_block.index),
                            virtual_column_layout: serialize_block.virtual_column_layout,
                        };
                        Ok(Event::Sync)
                    }
                }
                SerializeDataMeta::SerializeAppend => {
                    self.pending_merge_hll = true;
                    self.state = State::NeedSerialize {
                        block: input_data,
                        stats_type: ClusterStatsGenType::Generally,
                        index: None,
                        virtual_column_layout: None,
                    };
                    Ok(Event::Sync)
                }
                SerializeDataMeta::CompactExtras(compact_extras) => {
                    // compact extras
                    let data_block = Self::mutation_logs(
                        MutationLogEntry::CompactExtras {
                            extras: compact_extras,
                        },
                        0,
                        0,
                    );
                    self.output.push_data(Ok(data_block));
                    Ok(Event::NeedConsume)
                }
            }
        } else if input_data.is_empty() {
            // do nothing
            let data_block = Self::mutation_logs(MutationLogEntry::DoNothing, 0, 0);
            self.output.push_data(Ok(data_block));
            Ok(Event::NeedConsume)
        } else {
            self.state = State::NeedSerialize {
                block: input_data,
                stats_type: ClusterStatsGenType::Generally,
                index: None,
                virtual_column_layout: None,
            };
            Ok(Event::Sync)
        }
    }

    fn process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::Consume) {
            State::NeedSerialize {
                block,
                stats_type,
                index,
                virtual_column_layout,
            } => {
                // Check if the datablock is valid, this is needed to ensure data is correct
                block.check_valid()?;

                let mut block_builder = self.block_builder.clone();
                block_builder
                    .block_index_specs
                    .retain(|spec| match spec.index_name() {
                        Some(name) => !self
                            .recluster_merged_names
                            .iter()
                            .any(|merged| merged == name),
                        None => true,
                    });
                if let Some(layout) = virtual_column_layout
                    && let Some(builder) = block_builder.virtual_column_builder.take()
                {
                    block_builder.virtual_column_builder =
                        Some(builder.with_adaptive_layout(Arc::new(layout)));
                }
                let gen_stats = |block, generator: &ClusterStatsGenerator| match &stats_type {
                    ClusterStatsGenType::Generally => generator.gen_stats_for_append(block),
                    ClusterStatsGenType::WithOrigin(origin_stats) => {
                        generator.gen_with_origin_stats(block, origin_stats.clone())
                    }
                };
                if self.column_wise_upload {
                    let (serialized, parquet, schema) =
                        block_builder.build_column_wise(block, gen_stats)?;
                    let pending = Box::new(PendingColumnWise {
                        serialized,
                        index,
                        parquet: Some(parquet),
                        schema,
                        writer: None,
                        metadata: None,
                        written: 0,
                        start: Instant::now(),
                    });
                    self.state = Self::encode_next_column(pending)?;
                } else {
                    let serialized = block_builder.build(block, gen_stats)?;
                    self.state = State::Serialized { serialized, index };
                }
            }
            State::EncodeColumn(pending) => {
                self.state = Self::encode_next_column(pending)?;
            }
            _ => return Err(ErrorCode::Internal("It's a bug.")),
        }
        Ok(())
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::Consume) {
            State::Serialized { serialized, index } => {
                let merge_hll = std::mem::take(&mut self.pending_merge_hll);
                let (logical_updated_rows, logical_deleted_rows) =
                    std::mem::take(&mut self.pending_logical_change);
                let extended_block_meta = BlockWriter::write_down(&self.dal, serialized).await?;
                self.on_block_written(
                    extended_block_meta,
                    index,
                    merge_hll,
                    logical_updated_rows,
                    logical_deleted_rows,
                );
            }
            State::UploadColumn {
                mut pending,
                chunks,
            } => {
                if let Err(e) = Self::upload_chunks(&self.dal, &mut pending, chunks).await {
                    if let Some(writer) = pending.writer.as_mut() {
                        let _ = writer.abort().await;
                    }
                    return Err(e);
                }
                if pending.metadata.is_none() {
                    self.state = State::EncodeColumn(pending);
                    return Ok(());
                }

                let PendingColumnWise {
                    mut serialized,
                    index,
                    schema,
                    metadata,
                    written,
                    start,
                    ..
                } = *pending;
                serialized.block_meta.col_metas =
                    column_parquet_metas(&metadata.unwrap(), &schema)?;
                serialized.block_meta.file_size = written;
                metrics_inc_block_write_nums(1);
                metrics_inc_block_write_nums(written);
                metrics_inc_block_write_milliseconds(start.elapsed().as_millis() as u64);

                let merge_hll = std::mem::take(&mut self.pending_merge_hll);
                let (logical_updated_rows, logical_deleted_rows) =
                    std::mem::take(&mut self.pending_logical_change);
                let extended_block_meta =
                    BlockWriter::write_down_except_data(&self.dal, serialized).await?;
                self.on_block_written(
                    extended_block_meta,
                    index,
                    merge_hll,
                    logical_updated_rows,
                    logical_deleted_rows,
                );
            }
            _ => return Err(ErrorCode::Internal("It's a bug.")),
        }
        Ok(())
    }
}

impl TransformSerializeBlock {
    /// Encode the next column into an upload state; once all columns are written, produce the
    /// footer as the final upload.
    fn encode_next_column(mut pending: Box<PendingColumnWise>) -> Result<State> {
        let parquet = pending
            .parquet
            .as_mut()
            .ok_or_else(|| ErrorCode::Internal("column-wise parquet writer already finished"))?;
        let chunks = match parquet.write_next_column()? {
            Some(chunks) => chunks,
            None => {
                let (chunks, metadata) = pending.parquet.take().unwrap().finish()?;
                pending.metadata = Some(metadata);
                chunks
            }
        };
        Ok(State::UploadColumn { pending, chunks })
    }

    /// Push one column's (or the footer's) bytes into the multipart writer; close it after the
    /// footer. Opendal coalesces small writes up to the backend's minimum part size.
    async fn upload_chunks(
        dal: &Operator,
        pending: &mut PendingColumnWise,
        chunks: Vec<Bytes>,
    ) -> Result<()> {
        if pending.writer.is_none() {
            pending.writer = Some(
                dal.writer(&pending.serialized.block_meta.location.0)
                    .await?,
            );
        }
        let writer = pending.writer.as_mut().unwrap();
        let size: usize = chunks.iter().map(|c| c.len()).sum();
        if size > 0 {
            writer.write(chunks).await?;
            pending.written += size as u64;
        }
        if pending.metadata.is_some() {
            writer.close().await?;
        }
        Ok(())
    }

    fn on_block_written(
        &mut self,
        extended_block_meta: ExtendedBlockMeta,
        index: Option<BlockMetaIndex>,
        merge_hll: bool,
        logical_updated_rows: u64,
        logical_deleted_rows: u64,
    ) {
        let bytes =
            if let Some(draft_virtual_block_meta) = &extended_block_meta.draft_virtual_block_meta {
                (extended_block_meta.block_meta.block_size
                    + draft_virtual_block_meta
                        .virtual_columns
                        .as_ref()
                        .map(|meta| meta.virtual_column_size)
                        .unwrap_or_default()) as usize
            } else {
                extended_block_meta.block_meta.block_size as usize
            };
        let progress_values = ProgressValues {
            rows: extended_block_meta.block_meta.row_count as usize,
            bytes,
        };
        self.block_builder
            .ctx
            .get_write_progress()
            .incr(&progress_values);

        if let Some(rows) = self.recluster_index_rows.take() {
            metrics_inc_recluster_write_block_nums();
            self.output_data = Some(DataBlock::empty_with_meta(Box::new(ReclusterIndexOutput {
                meta: extended_block_meta,
                output_row: self.recluster_output_row,
                rows,
            })));
            self.recluster_merged_names.clear();
            return;
        }
        let mutation_log_data_block = if let Some(index) = index {
            // we are replacing the block represented by the `index`
            Self::mutation_logs(
                MutationLogEntry::ReplacedBlock {
                    index,
                    block_meta: Arc::new(extended_block_meta),
                },
                logical_updated_rows,
                logical_deleted_rows,
            )
        } else {
            // appending new data block
            if matches!(self.kind, MutationKind::Insert) {
                if self.table_id.is_none() {
                    self.block_builder
                        .ctx
                        .mutation_state()
                        .add_mutation_status(MutationStatus {
                            insert_rows: extended_block_meta.block_meta.row_count,
                            update_rows: 0,
                            deleted_rows: 0,
                        });
                }
            }

            if matches!(self.kind, MutationKind::Insert) {
                DataBlock::empty_with_meta(Box::new(extended_block_meta))
            } else {
                if matches!(self.kind, MutationKind::Recluster) {
                    metrics_inc_recluster_write_block_nums();
                }
                Self::mutation_logs(
                    MutationLogEntry::AppendBlock {
                        block_meta: Arc::new(extended_block_meta),
                        merge_hll,
                    },
                    logical_updated_rows,
                    logical_deleted_rows,
                )
            }
        };
        self.output_data = Some(mutation_log_data_block);
    }
}
