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
use std::mem;
use std::sync::Arc;

use arrow_schema::Schema;
use async_trait::async_trait;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::TableSchemaRef;
use databend_common_pipeline::core::Event;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline::core::ProcessorPtr;
use lance::dataset::CommitBuilder;
use lance::dataset::transaction::Operation as LanceOperation;
use lance::dataset::transaction::TransactionBuilder;
use lance::datatypes::Schema as LanceSchema;
use lance::io::ObjectStoreParams;
use lance_table::format::Fragment;
use lance_table::io::commit::UnsafeCommitHandler;
use log::info;
use opendal::Operator;

use super::writer_processor::LanceDatasetWriter;
use super::writer_processor::SharedFragmentState;

async fn commit_fragments_to_target_dataset(
    target_dataset_path: &str,
    schema: TableSchemaRef,
    store_params: ObjectStoreParams,
    fragments: Vec<Fragment>,
) -> Result<()> {
    if fragments.is_empty() {
        return Ok(());
    }

    let arrow_schema = Schema::from(schema.as_ref());
    let lance_schema = LanceSchema::try_from(&arrow_schema)
        .map_err(|err| ErrorCode::StorageOther(format!("lance schema conversion failed: {err}")))?;

    info!(
        "commit lance fragments into target dataset: path={}, fragments={}",
        target_dataset_path,
        fragments.len()
    );

    let transaction = TransactionBuilder::new(0, LanceOperation::Overwrite {
        schema: lance_schema,
        fragments,
        config_upsert_values: None,
        initial_bases: None,
    })
    .build();

    CommitBuilder::new(target_dataset_path)
        .with_store_params(store_params)
        .with_commit_handler(Arc::new(UnsafeCommitHandler))
        .execute(transaction)
        .await
        .map_err(|err| {
            ErrorCode::StorageOther(format!(
                "commit fragments to target lance dataset failed: {err}"
            ))
        })?;

    Ok(())
}

pub struct LanceDatasetCommitter {
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    target_dataset_path: String,
    target_store_params: ObjectStoreParams,
    schema: TableSchemaRef,
    fragment_state: Arc<SharedFragmentState>,

    input_data: Option<DataBlock>,
    pending_blocks: VecDeque<DataBlock>,
    output_blocks: Option<VecDeque<DataBlock>>,
    committed: bool,
}

impl LanceDatasetCommitter {
    pub fn try_create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        target_accessor: Operator,
        target_dataset_path: String,
        schema: TableSchemaRef,
        fragment_state: Arc<SharedFragmentState>,
    ) -> Result<ProcessorPtr> {
        let target_store_params =
            LanceDatasetWriter::build_store_params(target_accessor, target_dataset_path.as_str())?;

        Ok(ProcessorPtr::create(Box::new(Self {
            input,
            output,
            target_dataset_path,
            target_store_params,
            schema,
            fragment_state,
            input_data: None,
            pending_blocks: VecDeque::new(),
            output_blocks: None,
            committed: false,
        })))
    }
}

#[async_trait]
impl Processor for LanceDatasetCommitter {
    fn name(&self) -> String {
        "LanceDatasetCommitter".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.output.is_finished() {
            self.input.finish();
            Ok(Event::Finished)
        } else if let Some(blocks) = self.output_blocks.as_mut() {
            if self.output.can_push() {
                if let Some(block) = blocks.pop_front() {
                    self.output.push_data(Ok(block));
                    Ok(Event::NeedConsume)
                } else {
                    self.output_blocks = None;
                    self.output.finish();
                    Ok(Event::Finished)
                }
            } else {
                Ok(Event::NeedConsume)
            }
        } else if self.input_data.is_some() {
            self.input.set_not_need_data();
            Ok(Event::Sync)
        } else if self.input.has_data() {
            self.input_data = Some(self.input.pull_data().unwrap()?);
            self.input.set_not_need_data();
            Ok(Event::Sync)
        } else if self.input.is_finished() {
            if !self.committed {
                self.input.set_not_need_data();
                Ok(Event::Async)
            } else {
                self.output.finish();
                Ok(Event::Finished)
            }
        } else {
            self.input.set_need_data();
            Ok(Event::NeedData)
        }
    }

    fn process(&mut self) -> Result<()> {
        if let Some(block) = self.input_data.take() {
            self.pending_blocks.push_back(block);
        }
        Ok(())
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        if self.committed {
            return Ok(());
        }

        let fragments = self.fragment_state.take_fragments().await;
        commit_fragments_to_target_dataset(
            self.target_dataset_path.as_str(),
            self.schema.clone(),
            self.target_store_params.clone(),
            fragments,
        )
        .await?;

        self.output_blocks = Some(mem::take(&mut self.pending_blocks));
        self.committed = true;
        Ok(())
    }
}
