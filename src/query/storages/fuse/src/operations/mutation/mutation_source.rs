//  Copyright 2023 Datafuse Labs.
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

use common_base::base::tokio;
use common_catalog::plan::PartInfoPtr;
use common_catalog::table_context::TableContext;
use common_datablocks::DataBlock;
use common_exception::Result;

use crate::fuse_part::FusePartInfo;
use crate::io::BlockReader;
use crate::io::ReadSettings;
use crate::operations::mutation::MutationPartInfo;
use crate::operations::read::DataSourceMeta;
use crate::pipelines::processors::port::OutputPort;
use crate::pipelines::processors::processor::Event;
use crate::pipelines::processors::Processor;
use crate::MergeIOReadResult;

pub struct MutationSource {
    finished: bool,
    ctx: Arc<dyn TableContext>,
    batch_size: usize,
    block_reader: Arc<BlockReader>,

    output: Arc<OutputPort>,
    output_data: Option<(Vec<PartInfoPtr>, Vec<MergeIOReadResult>)>,
}

#[async_trait::async_trait]
impl Processor for MutationSource {
    fn name(&self) -> String {
        String::from("MutationSource")
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
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            return Ok(Event::NeedConsume);
        }

        if let Some((part, data)) = self.output_data.take() {
            let output = DataBlock::empty_with_meta(DataSourceMeta::create(part, data));
            self.output.push_data(Ok(output));
        }

        Ok(Event::Async)
    }

    async fn async_process(&mut self) -> Result<()> {
        let parts = self.ctx.try_get_parts(self.batch_size);

        if !parts.is_empty() {
            let mut chunks = Vec::with_capacity(parts.len());
            for part in &parts {
                let part = part.clone();
                let block_reader = self.block_reader.clone();
                let settings = ReadSettings::from_ctx(&self.ctx)?;

                chunks.push(async move {
                    tokio::spawn(async move {
                        let deletion_part = MutationPartInfo::from_part(&part)?;
                        let fuse_part = FusePartInfo::from_part(&deletion_part.inner_part)?;

                        block_reader
                            .read_columns_data_by_merge_io(
                                &settings,
                                &fuse_part.location,
                                &fuse_part.columns_meta,
                            )
                            .await
                    })
                    .await
                    .unwrap()
                });
            }

            self.output_data = Some((parts, futures::future::try_join_all(chunks).await?));
            return Ok(());
        }

        self.finished = true;
        Ok(())
    }
}
