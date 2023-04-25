// Copyright 2023 Datafuse Labs.
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
use std::sync::Arc;
use std::time::Instant;

use common_arrow::arrow::bitmap::MutableBitmap;
use common_base::base::ProgressValues;
use common_base::runtime::GlobalIORuntime;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::types::nullable::NullableColumn;
use common_expression::types::string::StringColumnBuilder;
use common_expression::types::variant::VariantType;
use common_expression::types::DataType;
use common_expression::BlockEntry;
use common_expression::Column;
use common_expression::DataBlock;
use common_expression::ScalarRef;
use common_expression::TableDataType;
use common_expression::TableField;
use common_expression::TableSchema;
use common_expression::TableSchemaRefExt;
use common_expression::Value;
use common_pipeline_core::processors::processor::ProcessorPtr;
use jsonb::array_length;
use jsonb::as_str;
use jsonb::get_by_index;
use jsonb::get_by_name;
use jsonb::object_keys;
use opendal::Operator;

use crate::io::write_data;
use crate::io::BlockBuilder;
use crate::io::BlockReader;
use crate::io::ReadSettings;
use crate::io::TableMetaLocationGenerator;
use crate::io::WriteSettings;
use crate::metrics::*;
use crate::operations::mutation::VirtualColumnMeta;
use crate::operations::mutation::VirtualColumnPartInfo;
use crate::pipelines::processors::port::OutputPort;
use crate::pipelines::processors::processor::Event;
use crate::pipelines::processors::Processor;
use crate::statistics::ClusterStatsGenerator;

pub struct VirtualColumnSource {
    ctx: Arc<dyn TableContext>,
    dal: Operator,

    meta_locations: TableMetaLocationGenerator,
    write_settings: WriteSettings,
    source_schema: Arc<TableSchema>,

    block_reader: Arc<BlockReader>,
    output: Arc<OutputPort>,
    output_data: Option<DataBlock>,
    finished: bool,
}

impl VirtualColumnSource {
    pub fn try_create(
        ctx: Arc<dyn TableContext>,
        dal: Operator,
        write_settings: WriteSettings,
        meta_locations: TableMetaLocationGenerator,
        source_schema: Arc<TableSchema>,
        block_reader: Arc<BlockReader>,
        output: Arc<OutputPort>,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Box::new(VirtualColumnSource {
            ctx,
            dal,
            meta_locations,
            write_settings,
            source_schema,
            block_reader,
            output,
            output_data: None,
            finished: false,
        })))
    }
}

#[async_trait::async_trait]
impl Processor for VirtualColumnSource {
    fn name(&self) -> String {
        "VirtualColumnSource".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.finished {
            self.output.finish();
            return Ok(Event::Finished);
        }

        if self.output.is_finished() {
            if !self.finished {
                return Ok(Event::Async);
            }
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            return Ok(Event::NeedConsume);
        }

        if let Some(block) = self.output_data.take() {
            self.output.push_data(Ok(block));
        }

        Ok(Event::Async)
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        match self.ctx.get_partition() {
            Some(part) => {
                let block_reader = self.block_reader.as_ref();

                let part = VirtualColumnPartInfo::from_part(&part)?;
                let block = &part.block;

                let settings = ReadSettings::from_ctx(&self.ctx)?;
                let storage_format = self.write_settings.storage_format;
                let start = Instant::now();
                // Perf
                {
                    metrics_inc_virtual_column_block_read_nums(1);
                    metrics_inc_virtual_column_block_read_bytes(block.block_size);
                }
                let block = block_reader
                    .read_by_meta(&settings, block.as_ref(), &storage_format)
                    .await?;

                // Perf.
                {
                    metrics_inc_virtual_column_block_read_milliseconds(
                        start.elapsed().as_millis() as u64
                    );
                }

                let mut keys = Vec::new();
                for (i, _field) in self.source_schema.fields().iter().enumerate() {
                    let block_entry = block.get_by_offset(i);
                    let column = block_entry
                        .value
                        .convert_to_full_column(&block_entry.data_type, block.num_rows());

                    let mut key_paths = BTreeMap::new();
                    for row in 0..block.num_rows() {
                        let val = unsafe { column.index_unchecked(row) };
                        if let ScalarRef::Variant(v) = val {
                            if let Some(keys) = object_keys(v) {
                                let len = array_length(&keys).unwrap();
                                for i in 0..len {
                                    let key = get_by_index(&keys, i as i32).unwrap();
                                    let key = as_str(&key).unwrap();
                                    let key_str = key.to_string();
                                    if let Some(cnt) = key_paths.get_mut(&key_str) {
                                        *cnt += 1;
                                    } else {
                                        key_paths.insert(key_str, 1);
                                    }
                                }
                            }
                        }
                    }
                    for (key, cnt) in key_paths.iter() {
                        if *cnt == block.num_rows() {
                            keys.push((i, key.clone()));
                        }
                    }
                }
                let len = block.num_rows();
                if !keys.is_empty() {
                    let mut virtual_fields = Vec::with_capacity(keys.len());
                    let mut virtual_columns = Vec::with_capacity(keys.len());
                    for (i, key) in keys {
                        let source_field = self.source_schema.field(i);
                        let virtual_name = format!("{}.{}", source_field.name(), key);
                        let virtual_field = TableField::new(
                            virtual_name.as_str(),
                            TableDataType::Nullable(Box::new(TableDataType::Variant)),
                        );
                        virtual_fields.push(virtual_field);

                        let block_entry = block.get_by_offset(i);
                        let column = block_entry
                            .value
                            .convert_to_full_column(&block_entry.data_type, len);

                        let mut validity = MutableBitmap::with_capacity(len);
                        let mut builder = StringColumnBuilder::with_capacity(len, len * 10);
                        for row in 0..len {
                            let val = unsafe { column.index_unchecked(row) };
                            if let ScalarRef::Variant(v) = val {
                                if let Some(inner_val) = get_by_name(&v, key.as_str()) {
                                    validity.push(true);
                                    builder.put_slice(inner_val.as_slice());
                                    builder.commit_row();
                                    continue;
                                }
                            }
                            validity.push(false);
                            builder.commit_row();
                        }
                        let column = Column::Nullable(Box::new(
                            NullableColumn::<VariantType> {
                                column: builder.build(),
                                validity: validity.into(),
                            }
                            .upcast(),
                        ));
                        let virtual_column = BlockEntry {
                            data_type: DataType::Nullable(Box::new(DataType::Variant)),
                            value: Value::Column(column),
                        };
                        virtual_columns.push(virtual_column);
                    }
                    let virtual_schema = TableSchemaRefExt::create(virtual_fields);
                    let virtual_block = DataBlock::new(virtual_columns, len);

                    let block_builder = BlockBuilder {
                        ctx: self.ctx.clone(),
                        meta_locations: self.meta_locations.clone(),
                        source_schema: virtual_schema,
                        write_settings: self.write_settings.clone(),
                        cluster_stats_gen: ClusterStatsGenerator::default(),
                    };

                    // build block serialization.
                    let serialized = GlobalIORuntime::instance()
                        .spawn_blocking(move || {
                            block_builder.build(virtual_block, |block, _| Ok((None, block)))
                        })
                        .await?;

                    // Perf.
                    {
                        metrics_inc_virtual_column_block_write_nums(1);
                        metrics_inc_virtual_column_block_write_bytes(
                            serialized.block_raw_data.len() as u64,
                        );
                    }

                    // write block data.
                    write_data(
                        serialized.block_raw_data,
                        &self.dal,
                        &serialized.block_meta.location.0,
                    )
                    .await?;

                    // Perf
                    {
                        metrics_inc_virtual_column_block_write_milliseconds(
                            start.elapsed().as_millis() as u64,
                        );
                    }

                    let progress_values = ProgressValues {
                        rows: serialized.block_meta.row_count as usize,
                        bytes: serialized.block_meta.block_size as usize,
                    };
                    self.ctx.get_write_progress().incr(&progress_values);

                    self.output_data = Some(DataBlock::empty_with_meta(VirtualColumnMeta::create(
                        part.index.clone(),
                        serialized.block_meta.into(),
                    )));
                } else {
                    self.output_data = None;
                }
            }
            None => self.finished = true,
        };

        Ok(())
    }
}
