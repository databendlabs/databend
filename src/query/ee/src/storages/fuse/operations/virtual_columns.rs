// Copyright 2023 Databend Cloud
//
// Licensed under the Elastic License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.elastic.co/licensing/elastic-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::VecDeque;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;
use std::time::Instant;

use databend_common_catalog::plan::Projection;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::local_block_meta_serde;
use databend_common_expression::BlockMetaInfo;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::ComputedExpr;
use databend_common_expression::DataBlock;
use databend_common_expression::TableDataType;
use databend_common_expression::TableSchema;
use databend_common_metrics::storage::metrics_inc_block_virtual_column_write_bytes;
use databend_common_metrics::storage::metrics_inc_block_virtual_column_write_milliseconds;
use databend_common_metrics::storage::metrics_inc_block_virtual_column_write_nums;
use databend_common_pipeline_core::Pipeline;
use databend_common_pipeline_sources::AsyncSource;
use databend_common_pipeline_sources::AsyncSourcer;
use databend_common_pipeline_transforms::processors::AsyncTransform;
use databend_common_pipeline_transforms::processors::TransformPipelineHelper;
use databend_common_sql::executor::physical_plans::MutationKind;
use databend_common_storages_fuse::io::read::read_segment_stats;
use databend_common_storages_fuse::io::write_data;
use databend_common_storages_fuse::io::BlockReader;
use databend_common_storages_fuse::io::MetaReaders;
use databend_common_storages_fuse::io::VirtualColumnBuilder;
use databend_common_storages_fuse::io::WriteSettings;
use databend_common_storages_fuse::operations::BlockMetaIndex;
use databend_common_storages_fuse::operations::CommitSink;
use databend_common_storages_fuse::operations::MutationGenerator;
use databend_common_storages_fuse::operations::MutationLogEntry;
use databend_common_storages_fuse::operations::MutationLogs;
use databend_common_storages_fuse::operations::TableMutationAggregator;
use databend_common_storages_fuse::FuseStorageFormat;
use databend_common_storages_fuse::FuseTable;
use databend_storages_common_cache::LoadParams;
use databend_storages_common_io::ReadSettings;
use databend_storages_common_table_meta::meta::BlockHLLState;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::ExtendedBlockMeta;
use databend_storages_common_table_meta::meta::RawBlockHLL;
use databend_storages_common_table_meta::meta::Statistics;
use opendal::Operator;

// The big picture of refresh virtual column into pipeline:
//
//                                    ┌─────────────────────────┐
//                             ┌────> │ VirtualColumnTransform1 │ ────┐
//                             │      └─────────────────────────┘     │
//                             │                  ...                 │
// ┌─────────────────────┐     │      ┌─────────────────────────┐     │      ┌─────────────────────────┐       ┌────────────┐
// │ VirtualColumnSource │ ────┼────> │ VirtualColumnTransformN │ ────┼────> │ TableMutationAggregator │ ────> │ CommitSink │
// └─────────────────────┘     │      └─────────────────────────┘     │      └─────────────────────────┘       └────────────┘
//                             │                  ...                 │
//                             │      ┌─────────────────────────┐     │
//                             └────> │ VirtualColumnTransformZ │ ────┘
//                                    └─────────────────────────┘
//
#[async_backtrace::framed]
pub async fn do_refresh_virtual_column(
    ctx: Arc<dyn TableContext>,
    fuse_table: &FuseTable,
    pipeline: &mut Pipeline,
) -> Result<()> {
    let Some(snapshot) = fuse_table.read_table_snapshot().await? else {
        // no snapshot
        return Ok(());
    };
    let table_schema = &fuse_table.get_table_info().meta.schema;

    // Collect source fields used by virtual columns.
    let mut fields = Vec::new();
    let mut field_indices = Vec::new();
    for (i, f) in table_schema.fields().iter().enumerate() {
        if f.data_type().remove_nullable() != TableDataType::Variant
            || matches!(f.computed_expr(), Some(ComputedExpr::Virtual(_)))
        {
            continue;
        }
        fields.push(f.clone());
        field_indices.push(i);
    }

    let source_schema = Arc::new(TableSchema {
        fields,
        ..fuse_table.schema().as_ref().clone()
    });

    if !fuse_table.support_virtual_columns() {
        return Err(ErrorCode::VirtualColumnError(
            "table don't support virtual column".to_string(),
        ));
    }
    let virtual_column_builder = VirtualColumnBuilder::try_create(ctx.clone(), source_schema)?;

    let projection = Projection::Columns(field_indices);
    let block_reader =
        fuse_table.create_block_reader(ctx.clone(), projection, false, false, false)?;

    let segment_reader =
        MetaReaders::segment_info_reader(fuse_table.get_operator(), table_schema.clone());

    let write_settings = fuse_table.get_write_settings();
    let storage_format = write_settings.storage_format;

    let operator = fuse_table.get_operator_ref();

    // Iterates through all segments and collect blocks don't have virtual block meta.
    let mut virtual_column_metas = VecDeque::new();
    for (segment_idx, (location, ver)) in snapshot.segments.iter().enumerate() {
        let segment_info = segment_reader
            .read(&LoadParams {
                location: location.to_string(),
                len_hint: None,
                ver: *ver,
                put_cache: false,
            })
            .await?;
        let stats = match &segment_info.summary.additional_stats_meta {
            Some(meta) if meta.location.is_some() => {
                Some(read_segment_stats(operator.clone(), meta.location.clone().unwrap()).await?)
            }
            _ => None,
        };

        for (block_idx, block_meta) in segment_info.block_metas()?.into_iter().enumerate() {
            if block_meta.virtual_block_meta.is_some() {
                continue;
            }
            virtual_column_metas.push_back(VirtualColumnMeta {
                index: BlockMetaIndex {
                    segment_idx,
                    block_idx,
                },
                block_meta,
                column_hlls: stats
                    .as_ref()
                    .and_then(|v| v.block_hlls.get(block_idx))
                    .cloned(),
            });
        }
    }

    if virtual_column_metas.is_empty() {
        return Ok(());
    }

    // Read source blocks.
    let settings = ReadSettings::from_ctx(&ctx)?;
    pipeline.add_source(
        |output| {
            let inner = VirtualColumnSource::new(
                settings,
                storage_format,
                block_reader.clone(),
                virtual_column_metas.clone(),
            );
            AsyncSourcer::create(ctx.clone(), output, inner)
        },
        1,
    )?;

    // Extract inner fields as virtual columns and write virtual block data.
    let block_nums = virtual_column_metas.len();
    let max_threads = ctx.get_settings().get_max_threads()? as usize;
    let max_threads = std::cmp::min(block_nums, max_threads);
    pipeline.try_resize(max_threads)?;
    pipeline.add_async_transformer(|| {
        VirtualColumnTransform::new(
            operator.clone(),
            write_settings.clone(),
            virtual_column_builder.clone(),
        )
    });

    pipeline.try_resize(1)?;
    let table_meta_timestamps =
        ctx.get_table_meta_timestamps(fuse_table, Some(snapshot.clone()))?;
    pipeline.add_async_accumulating_transformer(|| {
        TableMutationAggregator::create(
            fuse_table,
            ctx.clone(),
            snapshot.segments.clone(),
            vec![],
            vec![],
            Statistics::default(),
            MutationKind::Refresh,
            table_meta_timestamps,
        )
    });

    let prev_snapshot_id = snapshot.snapshot_id;
    let snapshot_gen = MutationGenerator::new(Some(snapshot), MutationKind::Refresh);
    pipeline.add_sink(|input| {
        CommitSink::try_create(
            fuse_table,
            ctx.clone(),
            None,
            vec![],
            snapshot_gen.clone(),
            input,
            None,
            Some(prev_snapshot_id),
            None,
            table_meta_timestamps,
        )
    })?;

    Ok(())
}

/// `VirtualColumnSource` is used to read data blocks that need generate virtual columns.
pub struct VirtualColumnSource {
    settings: ReadSettings,
    storage_format: FuseStorageFormat,
    block_reader: Arc<BlockReader>,
    virtual_column_metas: VecDeque<VirtualColumnMeta>,
    is_finished: bool,
}

impl VirtualColumnSource {
    pub fn new(
        settings: ReadSettings,
        storage_format: FuseStorageFormat,
        block_reader: Arc<BlockReader>,
        virtual_column_metas: VecDeque<VirtualColumnMeta>,
    ) -> Self {
        Self {
            settings,
            storage_format,
            block_reader,
            virtual_column_metas,
            is_finished: false,
        }
    }
}

#[async_trait::async_trait]
impl AsyncSource for VirtualColumnSource {
    const NAME: &'static str = "VirtualColumnSource";

    #[async_backtrace::framed]
    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        if self.is_finished {
            return Ok(None);
        }

        match self.virtual_column_metas.pop_front() {
            Some(meta) => {
                let block = self
                    .block_reader
                    .read_by_meta(&self.settings, &meta.block_meta, &self.storage_format)
                    .await?;
                let block = block.add_meta(Some(Box::new(meta)))?;
                Ok(Some(block))
            }
            None => {
                self.is_finished = true;
                Ok(None)
            }
        }
    }
}

/// `VirtualColumnTransform` is used to generate virtual columns for each blocks.
pub struct VirtualColumnTransform {
    operator: Operator,
    write_settings: WriteSettings,
    virtual_column_builder: VirtualColumnBuilder,
}

impl VirtualColumnTransform {
    pub fn new(
        operator: Operator,
        write_settings: WriteSettings,
        virtual_column_builder: VirtualColumnBuilder,
    ) -> Self {
        Self {
            operator,
            write_settings,
            virtual_column_builder,
        }
    }
}

#[async_trait::async_trait]
impl AsyncTransform for VirtualColumnTransform {
    const NAME: &'static str = "VirtualColumnTransform";

    #[async_backtrace::framed]
    async fn transform(&mut self, data_block: DataBlock) -> Result<DataBlock> {
        let VirtualColumnMeta {
            index,
            block_meta,
            column_hlls,
        } = data_block
            .get_meta()
            .and_then(VirtualColumnMeta::downcast_ref_from)
            .unwrap();

        self.virtual_column_builder.add_block(&data_block)?;
        let virtual_column_state = self
            .virtual_column_builder
            .finalize(&self.write_settings, &block_meta.location)?;

        if virtual_column_state
            .draft_virtual_block_meta
            .virtual_column_size
            > 0
        {
            let start = Instant::now();

            let virtual_column_size = virtual_column_state
                .draft_virtual_block_meta
                .virtual_column_size;
            let location = &virtual_column_state
                .draft_virtual_block_meta
                .virtual_location
                .0;

            write_data(virtual_column_state.data, &self.operator, location).await?;

            // Perf.
            {
                metrics_inc_block_virtual_column_write_nums(1);
                metrics_inc_block_virtual_column_write_bytes(virtual_column_size);
                metrics_inc_block_virtual_column_write_milliseconds(
                    start.elapsed().as_millis() as u64
                );
            }
        }

        let extended_block_meta = ExtendedBlockMeta {
            block_meta: Arc::unwrap_or_clone(block_meta.clone()),
            draft_virtual_block_meta: Some(virtual_column_state.draft_virtual_block_meta),
            column_hlls: column_hlls.clone().map(BlockHLLState::Serialized),
        };

        let entry = MutationLogEntry::ReplacedBlock {
            index: index.clone(),
            block_meta: Arc::new(extended_block_meta),
        };
        let meta = MutationLogs {
            entries: vec![entry],
        };
        let new_block = DataBlock::empty_with_meta(Box::new(meta));
        Ok(new_block)
    }
}

#[derive(Clone)]
pub struct VirtualColumnMeta {
    index: BlockMetaIndex,
    block_meta: Arc<BlockMeta>,
    column_hlls: Option<RawBlockHLL>,
}

impl Debug for VirtualColumnMeta {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        f.debug_struct("VirtualColumnMeta").finish()
    }
}

local_block_meta_serde!(VirtualColumnMeta);

#[typetag::serde(name = "virtual_column")]
impl BlockMetaInfo for VirtualColumnMeta {}
