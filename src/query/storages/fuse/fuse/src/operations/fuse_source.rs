//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::any::Any;
use std::sync::Arc;

use common_base::base::Progress;
use common_base::base::ProgressValues;
use common_catalog::plan::PartInfoPtr;
use common_catalog::plan::VirtualColumnInfo;
use common_catalog::table_context::TableContext;
use common_datablocks::DataBlock;
use common_datavalues::Column;
use common_datavalues::ColumnRef;
use common_datavalues::DataField;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataSchemaRefExt;
use common_datavalues::NullableColumn;
use common_datavalues::NullableColumnBuilder;
use common_datavalues::ScalarColumn;
use common_datavalues::Series;
use common_datavalues::VariantColumn;
use common_datavalues::VariantValue;
use common_exception::ErrorCode;
use common_exception::Result;
use common_functions::scalars::FunctionContext;
use common_jsonb::extract_value_by_path;
use common_jsonb::JsonPathRef;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_core::processors::Processor;
use common_sql::evaluator::EvalNode;

use crate::io::BlockReader;
use crate::operations::State::Generated;

type DataChunks = Vec<(usize, Vec<u8>)>;

pub struct PrewhereData {
    data_block: DataBlock,
    filter: ColumnRef,
}

pub enum State {
    ReadDataPrewhere(Option<PartInfoPtr>),
    ReadDataRemain(PartInfoPtr, PrewhereData),
    PrewhereFilter(PartInfoPtr, DataChunks),
    Deserialize(PartInfoPtr, DataChunks, Option<PrewhereData>),
    Generated(Option<PartInfoPtr>, DataBlock),
    Finish,
}

pub struct FuseTableSource {
    state: State,
    ctx: Arc<dyn TableContext>,
    scan_progress: Arc<Progress>,
    output: Arc<OutputPort>,
    output_reader: Arc<BlockReader>,

    prewhere_reader: Arc<BlockReader>,
    prewhere_filter: Arc<Option<EvalNode>>,
    remain_reader: Arc<Option<BlockReader>>,
    virtual_columns: Option<Vec<VirtualColumnInfo>>,
    prewhere_virtual_columns: Option<Vec<VirtualColumnInfo>>,

    support_blocking: bool,
}

impl FuseTableSource {
    #[allow(clippy::too_many_arguments)]
    pub fn create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        output_reader: Arc<BlockReader>,
        prewhere_reader: Arc<BlockReader>,
        prewhere_filter: Arc<Option<EvalNode>>,
        remain_reader: Arc<Option<BlockReader>>,
        virtual_columns: Option<Vec<VirtualColumnInfo>>,
        prewhere_virtual_columns: Option<Vec<VirtualColumnInfo>>,
    ) -> Result<ProcessorPtr> {
        let scan_progress = ctx.get_scan_progress();
        let support_blocking = prewhere_reader.support_blocking_api();
        Ok(ProcessorPtr::create(Box::new(FuseTableSource {
            ctx,
            output,
            scan_progress,
            state: State::ReadDataPrewhere(None),
            output_reader,
            prewhere_reader,
            prewhere_filter,
            remain_reader,
            virtual_columns,
            prewhere_virtual_columns,
            support_blocking,
        })))
    }

    fn generate_schema(&self) -> DataSchemaRef {
        match &self.virtual_columns {
            Some(virtual_columns) => {
                let mut fields = self.output_reader.schema().fields().clone();
                for virtual_column in virtual_columns {
                    let field =
                        DataField::new(&virtual_column.name, (*virtual_column.data_type).clone());
                    fields.push(field);
                }
                DataSchemaRefExt::create(fields)
            }
            None => self.output_reader.schema(),
        }
    }

    fn generate_prewhere_schema(&self) -> DataSchemaRef {
        match &self.prewhere_virtual_columns {
            Some(virtual_columns) => {
                let mut fields = self.prewhere_reader.schema().fields().clone();
                for virtual_column in virtual_columns {
                    let field =
                        DataField::new(&virtual_column.name, (*virtual_column.data_type).clone());
                    fields.push(field);
                }
                DataSchemaRefExt::create(fields)
            }
            None => self.prewhere_reader.schema(),
        }
    }

    fn generate_one_prewhere_block(&self, block: DataBlock) -> Result<DataBlock> {
        let block = if self.prewhere_virtual_columns.is_some() {
            let block = block.resort(self.prewhere_reader.schema())?;
            let schema = self.generate_prewhere_schema();
            let virtual_columns = self.prewhere_virtual_columns.as_ref().unwrap();
            // add virtual columns to the prewhere block
            self.generate_block_with_virtual_columns(block, schema, virtual_columns)?
        } else {
            block
        };
        Ok(block)
    }

    fn generate_one_block(&mut self, block: DataBlock) -> Result<()> {
        let new_part = self.ctx.try_get_part();
        // resort and prune columns
        let block = block.resort(self.output_reader.schema())?;
        let block = if self.virtual_columns.is_some() {
            let schema = self.generate_schema();
            let virtual_columns = self.virtual_columns.as_ref().unwrap();
            // add virtual columns to the block
            self.generate_block_with_virtual_columns(block, schema, virtual_columns)?
        } else {
            block
        };
        self.state = State::Generated(new_part, block);
        Ok(())
    }

    fn generate_one_empty_block(&mut self) -> Result<()> {
        let schema = self.generate_schema();
        let new_part = self.ctx.try_get_part();
        self.state = Generated(new_part, DataBlock::empty_with_schema(schema));
        Ok(())
    }

    fn generate_block_with_virtual_columns(
        &self,
        block: DataBlock,
        schema: DataSchemaRef,
        virtual_columns: &[VirtualColumnInfo],
    ) -> Result<DataBlock> {
        let mut columns = block.columns().to_vec();
        for virtual_column in virtual_columns.iter() {
            if let Ok(source_column) = block.try_column_by_name(&virtual_column.source_name) {
                let variant_column = if source_column.is_nullable() {
                    let nullable_column: &NullableColumn =
                        unsafe { Series::static_cast(source_column) };
                    let variant_column: &VariantColumn =
                        unsafe { Series::static_cast(nullable_column.inner()) };
                    variant_column
                } else {
                    let variant_column: &VariantColumn =
                        unsafe { Series::static_cast(source_column) };
                    variant_column
                };

                let mut builder =
                    NullableColumnBuilder::<VariantValue>::with_capacity(variant_column.len());
                let json_path_ref: Vec<JsonPathRef> = virtual_column
                    .json_path
                    .iter()
                    .map(|p| p.as_ref())
                    .collect();
                for value in variant_column.scalar_iter() {
                    match extract_value_by_path(value.as_ref(), &json_path_ref) {
                        Some(child_value) => {
                            builder.append(&VariantValue::from(child_value), true);
                        }
                        None => builder.append_null(),
                    }
                }
                let column = builder.build(variant_column.len());
                columns.push(column);
            }
        }
        Ok(DataBlock::create(schema, columns))
    }
}

#[async_trait::async_trait]
impl Processor for FuseTableSource {
    fn name(&self) -> String {
        "FuseEngineSource".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if matches!(self.state, State::ReadDataPrewhere(None)) {
            self.state = match self.ctx.try_get_part() {
                None => State::Finish,
                Some(part) => State::ReadDataPrewhere(Some(part)),
            }
        }

        if matches!(self.state, State::Finish) {
            self.output.finish();
            return Ok(Event::Finished);
        }

        if self.output.is_finished() {
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            return Ok(Event::NeedConsume);
        }

        if matches!(self.state, State::Generated(_, _)) {
            if let Generated(part, data_block) = std::mem::replace(&mut self.state, State::Finish) {
                self.state = match part {
                    None => State::Finish,
                    Some(part) => State::ReadDataPrewhere(Some(part)),
                };

                self.output.push_data(Ok(data_block));
                return Ok(Event::NeedConsume);
            }
        }

        match self.state {
            State::Finish => Ok(Event::Finished),
            State::ReadDataPrewhere(_) => {
                if self.support_blocking {
                    Ok(Event::Sync)
                } else {
                    Ok(Event::Async)
                }
            }
            State::ReadDataRemain(_, _) => {
                if self.support_blocking {
                    Ok(Event::Sync)
                } else {
                    Ok(Event::Async)
                }
            }
            State::PrewhereFilter(_, _) => Ok(Event::Sync),
            State::Deserialize(_, _, _) => Ok(Event::Sync),
            State::Generated(_, _) => Err(ErrorCode::Internal("It's a bug.")),
        }
    }

    fn process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::Finish) {
            State::Deserialize(part, chunks, prewhere_data) => {
                let data_block = if let Some(PrewhereData {
                    data_block: mut prewhere_blocks,
                    filter,
                }) = prewhere_data
                {
                    let block = if chunks.is_empty() {
                        prewhere_blocks
                    } else if let Some(remain_reader) = self.remain_reader.as_ref() {
                        let remain_block = remain_reader.deserialize(part, chunks)?;
                        for (col, field) in remain_block
                            .columns()
                            .iter()
                            .zip(remain_block.schema().fields())
                        {
                            prewhere_blocks =
                                prewhere_blocks.add_column(col.clone(), field.clone())?;
                        }
                        prewhere_blocks
                    } else {
                        return Err(ErrorCode::Internal("It's a bug. Need remain reader"));
                    };
                    // the last step of prewhere
                    let progress_values = ProgressValues {
                        rows: block.num_rows(),
                        bytes: block.memory_size(),
                    };
                    self.scan_progress.incr(&progress_values);
                    DataBlock::filter_block(block, &filter)?
                } else {
                    let block = self.output_reader.deserialize(part, chunks)?;
                    let progress_values = ProgressValues {
                        rows: block.num_rows(),
                        bytes: block.memory_size(),
                    };
                    self.scan_progress.incr(&progress_values);

                    block
                };

                self.generate_one_block(data_block)?;
                Ok(())
            }
            State::PrewhereFilter(part, chunks) => {
                // deserialize prewhere data block first
                let data_block = self.prewhere_reader.deserialize(part.clone(), chunks)?;
                let data_block = self.generate_one_prewhere_block(data_block)?;
                if let Some(filter) = self.prewhere_filter.as_ref() {
                    // do filter
                    let res = filter
                        .eval(&FunctionContext::default(), &data_block)?
                        .vector;
                    let filter = DataBlock::cast_to_nonull_boolean(&res)?;
                    // shortcut, if predicates is const boolean (or can be cast to boolean)
                    if !DataBlock::filter_exists(&filter)? {
                        // all rows in this block are filtered out
                        // turn to read next part
                        let progress_values = ProgressValues {
                            rows: data_block.num_rows(),
                            bytes: data_block.memory_size(),
                        };
                        self.scan_progress.incr(&progress_values);
                        self.generate_one_empty_block()?;
                        return Ok(());
                    }
                    if self.remain_reader.is_none() {
                        // shortcut, we don't need to read remain data
                        let progress_values = ProgressValues {
                            rows: data_block.num_rows(),
                            bytes: data_block.memory_size(),
                        };
                        self.scan_progress.incr(&progress_values);
                        let block = DataBlock::filter_block(data_block, &filter)?;
                        self.generate_one_block(block)?;
                    } else {
                        self.state =
                            State::ReadDataRemain(part, PrewhereData { data_block, filter });
                    }
                    Ok(())
                } else {
                    Err(ErrorCode::Internal(
                        "It's a bug. No need to do prewhere filter",
                    ))
                }
            }

            State::ReadDataPrewhere(Some(part)) => {
                let chunks = self.prewhere_reader.sync_read_columns_data(part.clone())?;

                if self.prewhere_filter.is_some() {
                    self.state = State::PrewhereFilter(part, chunks);
                } else {
                    // all needed columns are read.
                    self.state = State::Deserialize(part, chunks, None)
                }
                Ok(())
            }
            State::ReadDataRemain(part, prewhere_data) => {
                if let Some(remain_reader) = self.remain_reader.as_ref() {
                    let chunks = remain_reader.sync_read_columns_data(part.clone())?;
                    self.state = State::Deserialize(part, chunks, Some(prewhere_data));
                    Ok(())
                } else {
                    Err(ErrorCode::Internal("It's a bug. No remain reader"))
                }
            }
            _ => Err(ErrorCode::Internal("It's a bug.")),
        }
    }

    async fn async_process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::Finish) {
            State::ReadDataPrewhere(Some(part)) => {
                let chunks = self.prewhere_reader.read_columns_data(part.clone()).await?;

                if self.prewhere_filter.is_some() {
                    self.state = State::PrewhereFilter(part, chunks);
                } else {
                    // all needed columns are read.
                    self.state = State::Deserialize(part, chunks, None)
                }
                Ok(())
            }
            State::ReadDataRemain(part, prewhere_data) => {
                if let Some(remain_reader) = self.remain_reader.as_ref() {
                    let chunks = remain_reader.read_columns_data(part.clone()).await?;
                    self.state = State::Deserialize(part, chunks, Some(prewhere_data));
                    Ok(())
                } else {
                    Err(ErrorCode::Internal("It's a bug. No remain reader"))
                }
            }
            _ => Err(ErrorCode::Internal("It's a bug.")),
        }
    }
}
