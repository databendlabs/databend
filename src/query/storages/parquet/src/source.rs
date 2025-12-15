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
use std::borrow::Cow;
use std::collections::VecDeque;
use std::sync::Arc;

use bytes::Bytes;
use databend_common_base::base::Progress;
use databend_common_base::base::ProgressValues;
use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_catalog::plan::InternalColumnType;
use databend_common_catalog::plan::ParquetReadOptions;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::plan::TopK;
use databend_common_catalog::query_kind::QueryKind;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::TopKSorter;
use databend_common_pipeline::core::Event;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_storage::CopyStatus;
use databend_common_storage::FileStatus;
use databend_common_storage::OperatorRegistry;
use databend_storages_common_stage::add_internal_columns;
use parquet::arrow::parquet_to_arrow_schema;

use crate::ParquetFilePart;
use crate::ParquetPart;
use crate::ParquetReaderBuilder;
use crate::meta::check_parquet_schema;
use crate::meta::read_metadata_async_cached;
use crate::parquet_part::DeleteTask;
use crate::parquet_reader::ParquetWholeFileReader;
use crate::parquet_reader::RowGroupReader;
use crate::parquet_reader::cached_range_full_read;
use crate::parquet_reader::policy::ReadPolicyImpl;
use crate::partition::ParquetRowGroupPart;
use crate::read_settings::ReadSettings;

enum State {
    Init,
    // Reader, start row, location
    ReadRowGroup {
        readers: VecDeque<(ReadPolicyImpl, u64)>,
        location: String,
    },
    ReadFiles(Vec<(Bytes, String)>),
}

#[derive(Debug, Clone, Copy)]
pub enum ParquetSourceType {
    StageTable,
    ResultCache,
    Iceberg,
    DeltaLake,
    Hive,
    StreamingLoad,
}

impl ParquetSourceType {
    pub fn need_transformer(&self) -> bool {
        !matches!(
            self,
            ParquetSourceType::StageTable | ParquetSourceType::StreamingLoad
        )
    }
}

pub struct ParquetSource {
    source_type: ParquetSourceType,
    // Source processor related fields.
    output: Arc<OutputPort>,
    scan_progress: Arc<Progress>,

    // Used for event transforming.
    ctx: Arc<dyn TableContext>,
    generated_data: Option<DataBlock>,
    is_finished: bool,

    // Used to read parquet.
    row_group_reader: Arc<RowGroupReader>,
    whole_file_reader: Option<Arc<ParquetWholeFileReader>>,

    state: State,
    // If the source is used for a copy pipeline,
    // we should update copy status when reading small parquet files.
    // (Because we cannot collect copy status of small parquet files during `read_partition`).
    is_copy: bool,
    copy_status: Arc<CopyStatus>,
    /// Pushed-down topk sorter.
    topk_sorter: Option<TopKSorter>,

    internal_columns: Vec<InternalColumnType>,
    table_schema: TableSchemaRef,

    push_downs: Option<PushDownInfo>,
    topk: Arc<Option<TopK>>,
    op_registry: Arc<dyn OperatorRegistry>,
}

impl ParquetSource {
    #[allow(clippy::too_many_arguments)]
    pub fn create(
        ctx: Arc<dyn TableContext>,
        source_type: ParquetSourceType,
        output: Arc<OutputPort>,
        row_group_reader: Arc<RowGroupReader>,
        full_reader: Option<Arc<ParquetWholeFileReader>>,
        topk: Arc<Option<TopK>>,
        internal_columns: Vec<InternalColumnType>,
        push_downs: Option<PushDownInfo>,
        table_schema: TableSchemaRef,
        op_registry: Arc<dyn OperatorRegistry>,
    ) -> Result<ProcessorPtr> {
        let scan_progress = ctx.get_scan_progress();
        let is_copy = matches!(ctx.get_query_kind(), QueryKind::CopyIntoTable);
        let copy_status = ctx.get_copy_status();

        let topk_sorter = topk
            .as_ref()
            .as_ref()
            .map(|t| TopKSorter::new(t.limit, t.asc));

        Ok(ProcessorPtr::create(Box::new(Self {
            source_type,
            output,
            scan_progress,
            ctx,
            row_group_reader,
            whole_file_reader: full_reader,
            generated_data: None,
            is_finished: false,
            state: State::Init,
            is_copy,
            copy_status,
            topk_sorter,
            internal_columns,
            table_schema,
            push_downs,
            topk,
            op_registry,
        })))
    }
}

#[async_trait::async_trait]
impl Processor for ParquetSource {
    fn name(&self) -> String {
        "ParquetSource".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.is_finished {
            self.output.finish();
            return Ok(Event::Finished);
        }

        if self.output.is_finished() {
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            return Ok(Event::NeedConsume);
        }

        match self.generated_data.take() {
            None => match &self.state {
                State::Init => Ok(Event::Async),
                State::ReadRowGroup { .. } => Ok(Event::Sync),
                State::ReadFiles(_) => Ok(Event::Sync),
            },
            Some(data_block) => {
                let progress_values = ProgressValues {
                    rows: data_block.num_rows(),
                    bytes: data_block.memory_size(),
                };
                self.scan_progress.incr(&progress_values);
                Profile::record_usize_profile(
                    ProfileStatisticsName::ScanBytes,
                    data_block.memory_size(),
                );
                self.output.push_data(Ok(data_block));
                Ok(Event::NeedConsume)
            }
        }
    }

    fn process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::Init) {
            State::ReadRowGroup {
                readers: mut vs,
                location,
            } => {
                if let Some((reader, mut start_row)) = vs.front_mut() {
                    if let Some(mut block) = reader.as_mut().read_block()? {
                        add_internal_columns(
                            &self.internal_columns,
                            location.clone(),
                            &mut block,
                            &mut start_row,
                        );

                        if self.is_copy {
                            self.copy_status.add_chunk(location.as_str(), FileStatus {
                                num_rows_loaded: block.num_rows(),
                                error: None,
                            });
                        }
                        self.generated_data = Some(block);
                    } else {
                        vs.pop_front();
                    }
                    self.state = State::ReadRowGroup {
                        readers: vs,
                        location,
                    };
                }
                // Else: The reader is finished. We should try to build another reader.
            }
            State::ReadFiles(buffers) => {
                let mut blocks = Vec::with_capacity(buffers.len());
                for (buffer, path) in buffers {
                    let bs: Result<Vec<DataBlock>> = self
                        .whole_file_reader
                        .as_ref()
                        .unwrap()
                        .read_blocks_from_binary(buffer, &path)?
                        .collect();
                    let mut bs = bs?;

                    if self.is_copy {
                        let num_rows = bs.iter().map(|b| b.num_rows()).sum();
                        self.copy_status.add_chunk(path.as_str(), FileStatus {
                            num_rows_loaded: num_rows,
                            error: None,
                        });
                    }
                    let mut rows_start = 0;
                    for b in bs.iter_mut() {
                        add_internal_columns(
                            &self.internal_columns,
                            path.to_string(),
                            b,
                            &mut rows_start,
                        );
                    }
                    blocks.extend(bs);
                }

                if !blocks.is_empty() {
                    self.generated_data = Some(DataBlock::concat(&blocks)?);
                }
                // Else: no output data is generated.
            }
            _ => unreachable!(),
        }
        Ok(())
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::Init) {
            State::Init => {
                if let Some(part) = self.ctx.get_partition() {
                    match ParquetPart::from_part(&part)? {
                        // From Copy Table from Stage, we don't enable cache
                        ParquetPart::RowGroup(part) => {
                            if let Some(reader) = self
                                .row_group_reader
                                .create_read_policy(
                                    &ReadSettings::from_settings(&self.ctx.get_settings())?
                                        .with_enable_cache(!matches!(
                                            self.source_type,
                                            ParquetSourceType::StageTable
                                        )),
                                    part,
                                    &mut self.topk_sorter,
                                    None,
                                    &mut None,
                                )
                                .await?
                            {
                                self.state = State::ReadRowGroup {
                                    readers: vec![(reader, part.start_row)].into(),
                                    location: part.location.clone(),
                                };
                            }
                            // Else: keep in init state.
                        }
                        ParquetPart::SmallFiles(parts) => {
                            // read the small file on parallel
                            let mut handlers = Vec::with_capacity(parts.len());
                            for part in parts {
                                let (op, path) =
                                    self.row_group_reader.operator(part.file.as_str())?;

                                handlers.push(async move {
                                    let bs = cached_range_full_read(
                                        &op,
                                        path,
                                        part.compressed_size as _,
                                        false,
                                    )
                                    .await?;
                                    Ok::<_, ErrorCode>((bs, path.to_owned()))
                                });
                            }
                            let results = futures::future::try_join_all(handlers).await?;
                            self.state = State::ReadFiles(results);
                        }
                        ParquetPart::File(part) => {
                            let readers = self.get_rows_readers(part, None).await?;
                            if !readers.is_empty() {
                                self.state = State::ReadRowGroup {
                                    readers,
                                    location: part.file.clone(),
                                };
                            }
                        }
                        ParquetPart::FileWithDeletes { inner, deletes } => {
                            let readers = self.get_rows_readers(inner, Some(deletes)).await?;
                            if !readers.is_empty() {
                                self.state = State::ReadRowGroup {
                                    readers,
                                    location: inner.file.clone(),
                                };
                            }
                        }
                    }
                } else {
                    self.is_finished = true;
                }
            }
            _ => unreachable!(),
        }

        Ok(())
    }
}

impl ParquetSource {
    async fn get_rows_readers(
        &mut self,
        part: &ParquetFilePart,
        delete_files: Option<&[DeleteTask]>,
    ) -> Result<VecDeque<(ReadPolicyImpl, u64)>> {
        // Let's read the small file directly
        let (op, path) = self.row_group_reader.operator(part.file.as_str())?;
        // We should read the file with row group reader.
        let meta =
            read_metadata_async_cached(path, &op, Some(part.compressed_size), &part.dedup_key)
                .await?;

        let from_stage_table = matches!(self.source_type, ParquetSourceType::StageTable);
        if from_stage_table {
            check_parquet_schema(
                self.row_group_reader.schema_desc(),
                meta.file_metadata().schema_descr(),
                "first_file",
                part.file.as_str(),
            )?;
        }
        // The schema of the table in iceberg may be inconsistent with the schema in parquet
        let reader = if self.row_group_reader.schema_desc().root_schema()
            != meta.file_metadata().schema_descr().root_schema()
        {
            let read_options = ParquetReadOptions::default()
                .with_prune_row_groups(true)
                .with_prune_pages(false);

            let arrow_schema = parquet_to_arrow_schema(meta.file_metadata().schema_descr(), None)?;
            let need_row_number = self
                .internal_columns
                .contains(&InternalColumnType::FileRowNumber);
            let mut builder = ParquetReaderBuilder::create(
                self.ctx.clone(),
                self.op_registry.clone(),
                self.table_schema.clone(),
                arrow_schema,
            )?
            .with_options(read_options)
            .with_push_downs(self.push_downs.as_ref());

            if !need_row_number {
                builder = builder.with_topk(self.topk.as_ref().as_ref());
            }

            Cow::Owned(Arc::new(
                builder.build_row_group_reader(self.source_type, need_row_number)?,
            ))
        } else {
            Cow::Borrowed(&self.row_group_reader)
        };

        let should_read = |rowgroup_idx: usize, bucket_option: Option<(usize, usize)>| -> bool {
            if let Some((bucket, bucket_num)) = bucket_option {
                return rowgroup_idx % bucket_num == bucket;
            }
            true
        };

        let mut start_row = 0;
        let mut readers = VecDeque::with_capacity(meta.num_row_groups());
        // Deleted files only belong to the same Parquet, so they only need to be loaded once
        let mut buf_delete_selection = None;
        let delete_info = delete_files.as_ref().map(|tasks| (meta.as_ref(), *tasks));

        for (rowgroup_idx, rg) in meta.row_groups().iter().enumerate() {
            start_row += rg.num_rows() as u64;
            // filter by bucket option
            if !should_read(rowgroup_idx, part.bucket_option) {
                continue;
            }

            let part = ParquetRowGroupPart {
                location: part.file.clone(),
                start_row,
                meta: rg.clone(),
                schema_index: 0,
                uncompressed_size: rg.total_byte_size() as u64,
                compressed_size: rg.compressed_size() as u64,
                sort_min_max: None,
                omit_filter: false,
                page_locations: None,
                selectors: None,
            };

            let reader = reader
                .create_read_policy(
                    &ReadSettings::from_settings(&self.ctx.get_settings())?
                        .with_enable_cache(!from_stage_table),
                    &part,
                    &mut self.topk_sorter,
                    delete_info,
                    &mut buf_delete_selection,
                )
                .await?;

            if let Some(reader) = reader {
                readers.push_back((reader, part.start_row));
            }
        }
        Ok(readers)
    }
}
