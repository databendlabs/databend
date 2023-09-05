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
use std::vec;

use common_base::base::tokio::time::sleep;
use common_base::base::tokio::time::Duration;
use common_base::base::Progress;
use common_base::base::ProgressValues;
use common_catalog::plan::PartInfoPtr;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::filter_helper::FilterHelpers;
use common_expression::types::BooleanType;
use common_expression::types::DataType;
use common_expression::DataBlock;
use common_expression::DataSchemaRef;
use common_expression::Evaluator;
use common_expression::Expr;
use common_expression::Value;
use common_functions::BUILTIN_FUNCTIONS;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_core::processors::Processor;
use log::debug;
use opendal::Operator;

use crate::hive_parquet_block_reader::DataBlockDeserializer;
use crate::hive_parquet_block_reader::HiveBlockReader;
use crate::HiveBlockFilter;
use crate::HiveBlocks;
use crate::HivePartInfo;

struct PreWhereData {
    data_blocks: Vec<DataBlock>,
    valids: Vec<Value<BooleanType>>,
}

enum State {
    /// Read parquet file meta data
    /// IO bound
    ReadMeta(Option<PartInfoPtr>),

    /// Read prewhere blocks from data groups (without deserialization)
    /// IO bound
    ReadPrewhereData(HiveBlocks),

    /// Read remain blocks from data groups (without deserialization)
    /// IO bound
    ReadRemainData(HiveBlocks, PreWhereData),

    /// do prewhere filter on prewhere data, if data are filtered, trans to Generated state with empty datablocks,
    /// else trans to ReadRemainData
    /// CPU bound
    PrewhereFilter(HiveBlocks, DataBlockDeserializer),

    /// Deserialize remain block from the given data groups, concat prewhere and remain data blocks
    /// CPU bound
    Deserialize(HiveBlocks, DataBlockDeserializer, PreWhereData),

    /// indicates that data blocks are ready, and needs to be consumed
    Generated(HiveBlocks, Vec<DataBlock>),
    Finish,
}

pub struct HiveTableSource {
    state: State,
    ctx: Arc<dyn TableContext>,
    dal: Operator,
    scan_progress: Arc<Progress>,
    prewhere_block_reader: Arc<HiveBlockReader>,
    remain_reader: Arc<Option<HiveBlockReader>>,
    prewhere_filter: Arc<Option<Expr>>,
    output: Arc<OutputPort>,
    delay: usize,
    hive_block_filter: Arc<HiveBlockFilter>,

    /// The schema before output. Some fields might be removed when outputting.
    source_schema: DataSchemaRef,
    /// The final output schema
    output_schema: DataSchemaRef,
}

impl HiveTableSource {
    #[allow(clippy::too_many_arguments)]
    pub fn create(
        ctx: Arc<dyn TableContext>,
        dal: Operator,
        output: Arc<OutputPort>,
        prewhere_block_reader: Arc<HiveBlockReader>,
        remain_reader: Arc<Option<HiveBlockReader>>,
        prewhere_filter: Arc<Option<Expr>>,
        delay: usize,
        hive_block_filter: Arc<HiveBlockFilter>,
        source_schema: DataSchemaRef,
        output_schema: DataSchemaRef,
    ) -> Result<ProcessorPtr> {
        let scan_progress = ctx.get_scan_progress();
        Ok(ProcessorPtr::create(Box::new(HiveTableSource {
            ctx,
            dal,
            output,
            prewhere_block_reader,
            remain_reader,
            prewhere_filter,
            hive_block_filter,
            scan_progress,
            state: State::ReadMeta(None),
            delay,
            source_schema,
            output_schema,
        })))
    }

    fn try_get_partitions(&mut self) {
        self.state = self
            .ctx
            .get_partition()
            .map_or(State::Finish, |part_info| State::ReadMeta(Some(part_info)));
    }

    fn exec_prewhere_filter(
        &self,
        filter: &Expr,
        data_blocks: &Vec<DataBlock>,
    ) -> Result<(bool, Vec<Value<BooleanType>>)> {
        assert_eq!(filter.data_type(), &DataType::Boolean);

        let mut valids = vec![];
        let mut exists = false;
        let func_ctx = self.ctx.get_function_context()?;
        for datablock in data_blocks {
            let evaluator = Evaluator::new(datablock, &func_ctx, &BUILTIN_FUNCTIONS);
            let predicates = evaluator
                .run(filter)
                .map_err(|e| e.add_message("eval prewhere filter failed:"))?
                .try_downcast::<BooleanType>()
                .unwrap();

            // shortcut, if predicates is const boolean (or can be cast to boolean)
            if !FilterHelpers::is_all_unset(&predicates) {
                exists = true;
            }

            valids.push(predicates);
        }

        assert_eq!(data_blocks.len(), valids.len());

        Ok((exists, valids))
    }

    fn do_prewhere_filter(
        &mut self,
        hive_blocks: HiveBlocks,
        rowgroup_deserializer: DataBlockDeserializer,
    ) -> Result<()> {
        // 1. deserialize chunks to datablocks
        let prewhere_datablocks = self
            .prewhere_block_reader
            .get_all_datablocks(rowgroup_deserializer, &hive_blocks.part)?;

        let progress_values = ProgressValues {
            rows: prewhere_datablocks.iter().map(|x| x.num_rows()).sum(),
            bytes: prewhere_datablocks.iter().map(|x| x.memory_size()).sum(),
        };
        self.scan_progress.incr(&progress_values);

        if let Some(filter) = self.prewhere_filter.as_ref() {
            // 2. do filter
            let (exists, valids) = self.exec_prewhere_filter(filter, &prewhere_datablocks)?;
            // 3. if all data filter out, try next rowgroup, trans to prewhere data
            if !exists {
                // all rows in this block are filtered out
                // turn to begin the next state cycle.
                // Generate a empty block.
                self.state = State::Generated(hive_blocks, vec![]);
                return Ok(());
            }
            // 4. if remain block is non, trans to generated state
            if self.remain_reader.is_none() {
                let prewhere_datablocks = prewhere_datablocks
                    .into_iter()
                    .zip(valids.iter())
                    .map(|(datablock, valid)| {
                        let datablock = DataBlock::filter_boolean_value(datablock, valid).unwrap();
                        datablock
                            .resort(&self.source_schema, &self.output_schema)
                            .unwrap()
                    })
                    .filter(|x| !x.is_empty())
                    .collect();

                self.state = State::Generated(hive_blocks, prewhere_datablocks);
            } else {
                // 5. if not all data filter out, and remain block reader is not non, trans to read remain
                self.state = State::ReadRemainData(hive_blocks, PreWhereData {
                    data_blocks: prewhere_datablocks,
                    valids,
                });
            }
        } else {
            // if no prewhere filter, data should be all fetched in prewhere state
            self.state = State::Generated(hive_blocks, prewhere_datablocks);
        }

        Ok(())
    }

    fn do_deserialize(
        &mut self,
        hive_blocks: HiveBlocks,
        rowgroup_deserializer: DataBlockDeserializer,
        prewhere_data: PreWhereData,
    ) -> Result<()> {
        let datablocks = if let Some(remain_reader) = self.remain_reader.as_ref() {
            // 1. deserialize all remain data block
            let remain_datablocks =
                remain_reader.get_all_datablocks(rowgroup_deserializer, &hive_blocks.part)?;
            // 2. concat prewhere and remain datablock(may be none)
            assert_eq!(remain_datablocks.len(), prewhere_data.data_blocks.len());

            let allblocks = remain_datablocks
                .iter()
                .zip(prewhere_data.data_blocks.iter())
                .zip(prewhere_data.valids.iter())
                .map(|((r, p), v)| {
                    // do merge block
                    assert_eq!(r.num_rows(), p.num_rows());
                    let mut a = p.clone();
                    for column in r.columns().iter() {
                        a.add_column(column.clone());
                    }
                    let a = DataBlock::filter_boolean_value(a, v).unwrap();
                    a.resort(&self.source_schema, &self.output_schema).unwrap()
                })
                .filter(|x| !x.is_empty())
                .collect::<Vec<_>>();
            allblocks
        } else {
            return Err(ErrorCode::Internal("It's a bug. No remain reader"));
        };

        // 3  trans to generate state
        self.state = State::Generated(hive_blocks, datablocks);
        Ok(())
    }
}

#[async_trait::async_trait]
impl Processor for HiveTableSource {
    fn name(&self) -> String {
        "HiveEngineSource".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if matches!(self.state, State::ReadMeta(None)) {
            self.try_get_partitions();
        }

        if self.output.is_finished() {
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            return Ok(Event::NeedConsume);
        }

        if matches!(self.state, State::Generated(_, _)) {
            if let State::Generated(mut hive_blocks, mut data_blocks) =
                std::mem::replace(&mut self.state, State::Finish)
            {
                // 1. consume all generated blocks,
                if let Some(data_block) = data_blocks.pop() {
                    self.output.push_data(Ok(data_block));
                    // 2. if not all consumed, retain generated state
                    self.state = State::Generated(hive_blocks, data_blocks);
                    return Ok(Event::NeedConsume);
                }

                // 3. if all consumed, try next rowgroup
                hive_blocks.advance();
                match hive_blocks.has_blocks() {
                    true => {
                        self.state = State::ReadPrewhereData(hive_blocks);
                    }
                    false => {
                        self.try_get_partitions();
                    }
                }
            }
        }

        match self.state {
            State::Finish => {
                self.output.finish();
                Ok(Event::Finished)
            }
            State::ReadMeta(_) => Ok(Event::Async),
            State::ReadPrewhereData(_) => Ok(Event::Async),
            State::ReadRemainData(_, _) => Ok(Event::Async),
            State::PrewhereFilter(_, _) => Ok(Event::Sync),
            State::Deserialize(_, _, _) => Ok(Event::Sync),
            State::Generated(_, _) => Err(ErrorCode::Internal("It's a bug.")),
        }
    }

    fn process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::Finish) {
            State::PrewhereFilter(hive_blocks, rowgroup_deserializer) => {
                self.do_prewhere_filter(hive_blocks, rowgroup_deserializer)
            }
            State::Deserialize(hive_blocks, rowgroup_deserializer, prewhere_data) => {
                self.do_deserialize(hive_blocks, rowgroup_deserializer, prewhere_data)
            }
            _ => Err(ErrorCode::Internal("It's a bug.")),
        }
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::Finish) {
            State::ReadMeta(Some(part)) => {
                if self.delay > 0 {
                    sleep(Duration::from_millis(self.delay as u64)).await;
                    debug!("sleep for {}ms", self.delay);
                    self.delay = 0;
                }
                let part = HivePartInfo::from_part(&part)?;
                let file_meta = self
                    .prewhere_block_reader
                    .read_meta_data(self.dal.clone(), &part.filename, part.filesize)
                    .await?;
                let mut hive_blocks =
                    HiveBlocks::create(file_meta, part.clone(), self.hive_block_filter.clone());

                match hive_blocks.prune() {
                    true => {
                        self.state = State::ReadPrewhereData(hive_blocks);
                    }
                    false => {
                        self.try_get_partitions();
                    }
                }
                Ok(())
            }
            State::ReadPrewhereData(hive_blocks) => {
                let row_group = hive_blocks.get_current_row_group_meta_data();
                let part = hive_blocks.get_part_info();
                let chunks = self
                    .prewhere_block_reader
                    .read_columns_data(row_group, &part)
                    .await?;
                let rowgroup_deserializer = self
                    .prewhere_block_reader
                    .create_rowgroup_deserializer(chunks, row_group)?;
                self.state = State::PrewhereFilter(hive_blocks, rowgroup_deserializer);
                Ok(())
            }

            State::ReadRemainData(hive_blocks, prewhere_data) => {
                let row_group = hive_blocks.get_current_row_group_meta_data();
                let part = hive_blocks.get_part_info();

                if let Some(remain_reader) = self.remain_reader.as_ref() {
                    let chunks = remain_reader.read_columns_data(row_group, &part).await?;
                    let rowgroup_deserializer =
                        remain_reader.create_rowgroup_deserializer(chunks, row_group)?;
                    self.state =
                        State::Deserialize(hive_blocks, rowgroup_deserializer, prewhere_data);
                    Ok(())
                } else {
                    Err(ErrorCode::Internal("It's a bug. No remain reader"))
                }
            }
            _ => Err(ErrorCode::Internal("It's a bug.")),
        }
    }
}
