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

use databend_common_base::base::Progress;
use databend_common_base::base::ProgressValues;
use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_catalog::plan::TopK;
use databend_common_catalog::query_kind::QueryKind;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::TopKSorter;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_storage::CopyStatus;
use databend_common_storage::FileStatus;

use super::parquet_reader::policy::ReadPolicyImpl;
use crate::ParquetPart;
use crate::ParquetRSFullReader;
use crate::ParquetRSRowGroupReader;
use crate::ReadSettings;

enum State {
    Init,
    ReadRowGroup(ReadPolicyImpl),
    ReadFiles(Vec<(String, Vec<u8>)>),
}

pub struct ParquetSource {
    // Source processor related fields.
    output: Arc<OutputPort>,
    scan_progress: Arc<Progress>,

    // Used for event transforming.
    ctx: Arc<dyn TableContext>,
    generated_data: Option<DataBlock>,
    is_finished: bool,

    // Used to read parquet.
    row_group_reader: Arc<ParquetRSRowGroupReader>,
    full_file_reader: Option<Arc<ParquetRSFullReader>>,

    state: State,
    // If the source is used for a copy pipeline,
    // we should update copy status when reading small parquet files.
    // (Because we cannot collect copy status of small parquet files during `read_partition`).
    is_copy: bool,
    copy_status: Arc<CopyStatus>,
    /// Pushed-down topk sorter.
    topk_sorter: Option<TopKSorter>,
}

impl ParquetSource {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        row_group_reader: Arc<ParquetRSRowGroupReader>,
        full_file_reader: Option<Arc<ParquetRSFullReader>>,
        topk: Arc<Option<TopK>>,
    ) -> Result<ProcessorPtr> {
        let scan_progress = ctx.get_scan_progress();
        let is_copy = matches!(ctx.get_query_kind(), QueryKind::CopyIntoTable);
        let copy_status = ctx.get_copy_status();

        let topk_sorter = topk
            .as_ref()
            .as_ref()
            .map(|t| TopKSorter::new(t.limit, t.asc));

        Ok(ProcessorPtr::create(Box::new(Self {
            output,
            scan_progress,
            ctx,
            row_group_reader,
            generated_data: None,
            is_finished: false,
            state: State::Init,
            is_copy,
            copy_status,
            topk_sorter,
            full_file_reader,
        })))
    }
}

#[async_trait::async_trait]
impl Processor for ParquetSource {
    fn name(&self) -> String {
        "ParquetRSSource".to_string()
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
                State::ReadFiles(_) => Ok(Event::Sync),
                State::ReadRowGroup(_) => Ok(Event::Sync),
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
            State::ReadRowGroup(mut reader) => {
                if let Some(block) = reader.as_mut().read_block()? {
                    self.generated_data = Some(block);
                    self.state = State::ReadRowGroup(reader);
                }
                // Else: The reader is finished. We should try to build another reader.
            }
            State::ReadFiles(buffers) => {
                let mut blocks = Vec::with_capacity(buffers.len());
                // Write `if` outside to reduce branches.
                if self.is_copy {
                    for (path, buffer) in buffers {
                        let bs = self
                            .full_file_reader
                            .as_ref()
                            .unwrap()
                            .read_blocks_from_binary(buffer)?;
                        let num_rows = bs.iter().map(|b| b.num_rows()).sum();
                        self.copy_status.add_chunk(path.as_str(), FileStatus {
                            num_rows_loaded: num_rows,
                            error: None,
                        });
                        blocks.extend(bs);
                    }
                } else {
                    for (_, buffer) in buffers {
                        blocks.extend(
                            self.full_file_reader
                                .as_ref()
                                .unwrap()
                                .read_blocks_from_binary(buffer)?,
                        );
                    }
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
                        ParquetPart::ParquetRSRowGroup(part) => {
                            if let Some(reader) = self
                                .row_group_reader
                                .create_read_policy(
                                    &ReadSettings::from_ctx(&self.ctx)?,
                                    part,
                                    &mut self.topk_sorter,
                                )
                                .await?
                            {
                                self.state = State::ReadRowGroup(reader);
                            }
                            // Else: keep in init state.
                        }
                        ParquetPart::ParquetFiles(parts) => {
                            let mut handlers = Vec::with_capacity(parts.files.len());
                            for (path, _) in parts.files.iter() {
                                let op = self.row_group_reader.operator();
                                let path = path.clone();
                                handlers.push(async move {
                                    // TODO: we can use opendal::Buffer to reduce memory alloc.
                                    let data = op.read(&path).await?.to_vec();
                                    Ok::<_, ErrorCode>((path, data))
                                });
                            }
                            let buffers = futures::future::try_join_all(handlers).await?;
                            self.state = State::ReadFiles(buffers);
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
