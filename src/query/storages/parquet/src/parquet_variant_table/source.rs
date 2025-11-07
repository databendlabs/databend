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
use std::collections::VecDeque;
use std::sync::Arc;

use bytes::Bytes;
use databend_common_base::base::Progress;
use databend_common_base::base::ProgressValues;
use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_catalog::plan::InternalColumnType;
use databend_common_catalog::query_kind::QueryKind;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::binary::BinaryColumnBuilder;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchema;
use databend_common_expression::TableDataType;
use databend_common_expression::TableSchema;
use databend_common_pipeline::core::Event;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_storage::CopyStatus;
use databend_common_storage::FileStatus;
use databend_common_storage::OperatorRegistry;
use databend_storages_common_stage::add_internal_columns;
use databend_storages_common_stage::read_record_batch_to_variant_column;
use databend_storages_common_stage::record_batch_to_variant_block;
use jiff::tz::TimeZone;
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::arrow::arrow_reader::ParquetRecordBatchReader;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::parquet_to_arrow_field_levels;
use parquet::arrow::parquet_to_arrow_schema;
use parquet::arrow::ProjectionMask;

use crate::meta::read_metadata_async_cached;
use crate::parquet_reader::cached_range_full_read;
use crate::parquet_reader::InMemoryRowGroup;
use crate::read_settings::ReadSettings;
use crate::schema::arrow_to_table_schema;
use crate::ParquetFilePart;
use crate::ParquetPart;

enum State {
    Init,
    // Reader, start row, location
    ReadRowGroup {
        readers: VecDeque<(ParquetRecordBatchReader, u64, TableDataType, DataSchema)>,
        location: String,
    },
    ReadFiles(Vec<(Bytes, String)>),
}

pub struct ParquetVariantSource {
    output: Arc<OutputPort>,
    scan_progress: Arc<Progress>,

    // Used for event transforming.
    ctx: Arc<dyn TableContext>,
    generated_data: Option<DataBlock>,
    is_finished: bool,

    state: State,
    // If the source is used for a copy pipeline,
    // we should update copy status when reading small parquet files.
    // (Because we cannot collect copy status of small parquet files during `read_partition`).
    is_copy: bool,
    copy_status: Arc<CopyStatus>,
    internal_columns: Vec<InternalColumnType>,
    op_registry: Arc<dyn OperatorRegistry>,
    batch_size: usize,
    use_logic_type: bool,

    tz: TimeZone,
}

impl ParquetVariantSource {
    #[allow(clippy::too_many_arguments)]
    pub fn try_create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        internal_columns: Vec<InternalColumnType>,
        op_registry: Arc<dyn OperatorRegistry>,
        use_logic_type: bool,
    ) -> Result<ProcessorPtr> {
        let scan_progress = ctx.get_scan_progress();
        let is_copy = matches!(ctx.get_query_kind(), QueryKind::CopyIntoTable);
        let copy_status = ctx.get_copy_status();

        let settings = ctx.get_settings();
        let tz_string = settings.get_timezone()?;
        let tz = TimeZone::get(&tz_string).map_err(|e| {
            ErrorCode::InvalidTimezone(format!("[QUERY-CTX] Timezone validation failed: {}", e))
        })?;

        Ok(ProcessorPtr::create(Box::new(Self {
            output,
            scan_progress,
            ctx,
            generated_data: None,
            is_finished: false,
            state: State::Init,
            is_copy,
            copy_status,
            internal_columns,
            op_registry,
            batch_size: 1000,
            tz,
            use_logic_type,
        })))
    }
}

#[async_trait::async_trait]
impl Processor for ParquetVariantSource {
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
                if let Some((reader, mut start_row, typ, data_schema)) = vs.front_mut() {
                    if let Some(batch) = reader.next() {
                        let mut block =
                            record_batch_to_variant_block(batch?, &self.tz, typ, data_schema)?;
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
                    let mut block =
                        read_small_file(buffer, self.batch_size, &self.tz, self.use_logic_type)?;

                    if self.is_copy {
                        self.copy_status.add_chunk(path.as_str(), FileStatus {
                            num_rows_loaded: block.num_rows(),
                            error: None,
                        });
                    }
                    let mut rows_start = 0;
                    add_internal_columns(
                        &self.internal_columns,
                        path.to_string(),
                        &mut block,
                        &mut rows_start,
                    );
                    blocks.push(block);
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
                        ParquetPart::SmallFiles(parts) => {
                            // read the small file on parallel
                            let mut handlers = Vec::with_capacity(parts.len());
                            for part in parts {
                                let (op, path) =
                                    self.op_registry.get_operator_path(part.file.as_str())?;
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
                            let readers = self.get_row_group_readers(part).await?;
                            if !readers.is_empty() {
                                self.state = State::ReadRowGroup {
                                    readers,
                                    location: part.file.clone(),
                                };
                            }
                        }
                        _ => unreachable!(),
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

impl ParquetVariantSource {
    async fn get_row_group_readers(
        &mut self,
        part: &ParquetFilePart,
    ) -> Result<VecDeque<(ParquetRecordBatchReader, u64, TableDataType, DataSchema)>> {
        let (op, path) = self.op_registry.get_operator_path(part.file.as_str())?;
        let meta =
            read_metadata_async_cached(path, &op, Some(part.compressed_size), &part.dedup_key)
                .await?;
        let field_levels = parquet_to_arrow_field_levels(
            meta.file_metadata().schema_descr(),
            ProjectionMask::all(),
            None,
        )?;
        let arrow_schema = parquet_to_arrow_schema(
            meta.file_metadata().schema_descr(),
            meta.file_metadata().key_value_metadata(),
        )?;
        let schema = arrow_to_table_schema(&arrow_schema, true, self.use_logic_type)?;
        let typ = schema_to_tuple_type(&schema);
        let data_schema = DataSchema::from(&schema);

        let should_read = |rowgroup_idx: usize, bucket_option: Option<(usize, usize)>| -> bool {
            if let Some((bucket, bucket_num)) = bucket_option {
                return rowgroup_idx % bucket_num == bucket;
            }
            true
        };

        let mut start_row = 0;
        let mut readers = VecDeque::with_capacity(meta.num_row_groups());
        for (rowgroup_idx, rg) in meta.row_groups().iter().enumerate() {
            start_row += rg.num_rows() as u64;
            // filter by bucket option
            if !should_read(rowgroup_idx, part.bucket_option) {
                continue;
            }
            let mut row_group =
                InMemoryRowGroup::new(&part.file, op.clone(), rg, None, ReadSettings::default());
            row_group.fetch(&ProjectionMask::all(), None).await?;
            let reader = ParquetRecordBatchReader::try_new_with_row_groups(
                &field_levels,
                &row_group,
                self.batch_size,
                None,
            )?;
            readers.push_back((reader, start_row, typ.clone(), data_schema.clone()));
        }
        Ok(readers)
    }
}

fn schema_to_tuple_type(schema: &TableSchema) -> TableDataType {
    TableDataType::Tuple {
        fields_name: schema.fields.iter().map(|f| f.name.clone()).collect(),
        fields_type: schema.fields.iter().map(|f| f.data_type.clone()).collect(),
    }
}

pub fn read_small_file(
    bytes: Bytes,
    batch_size: usize,
    tz: &TimeZone,
    use_logic_type: bool,
) -> databend_common_exception::Result<DataBlock> {
    let len = bytes.len();
    let builder =
        ParquetRecordBatchReaderBuilder::try_new_with_options(bytes, ArrowReaderOptions::new())?
            .with_batch_size(batch_size);

    // Prune row groups.
    let schema = arrow_to_table_schema(builder.schema(), true, use_logic_type)?;
    let typ = schema_to_tuple_type(&schema);
    let data_schema = DataSchema::from(&schema);
    let reader = builder.build()?;
    let mut builder = BinaryColumnBuilder::with_capacity(batch_size, len);
    for batch in reader {
        let batch = batch?;
        read_record_batch_to_variant_column(batch, &mut builder, tz, &typ, &data_schema)?;
    }
    let column = builder.build();
    let num_rows = column.len();
    Ok(DataBlock::new(
        vec![Column::Variant(column).into()],
        num_rows,
    ))
}
