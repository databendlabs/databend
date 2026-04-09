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
use databend_storages_common_stage::CopyIntoLocationInfo;
use lance_core::datatypes::Schema as LanceSchema;
use lance_file::format::MAGIC;
use lance_file::format::MAJOR_VERSION;
use lance_file::format::MINOR_VERSION;
use lance_io::object_store::ObjectStore as LanceObjectStore;
use lance_io::object_store::ObjectStoreParams;
use lance_io::object_store::ObjectStoreRegistry;
use lance_io::object_writer::ObjectWriter;
use lance_io::traits::WriteExt;
use lance_table::format::DataStorageFormat;
use lance_table::format::Fragment;
use lance_table::format::Manifest;
use log::info;
use opendal::Operator;

use super::writer_processor::FragmentWriterParams;
use super::writer_processor::SharedFragmentState;
use super::writer_processor::lance_compatible_arrow_schema;
use crate::append::UnloadOutput;
use crate::append::output::DataSummary;

async fn commit_fragments_to_target_dataset(
    target_dataset_path: &str,
    schema: TableSchemaRef,
    store_params: ObjectStoreParams,
    fragments: Vec<Fragment>,
) -> lance_core::Result<()> {
    if fragments.is_empty() {
        return Ok(());
    }

    let arrow_schema = lance_compatible_arrow_schema(&Schema::from(schema.as_ref()));
    let lance_schema = LanceSchema::try_from(&arrow_schema)?;

    info!(
        "commit lance fragments into target dataset: path={}, fragments={}",
        target_dataset_path,
        fragments.len()
    );

    let mut fragment_id = 0;
    let fragments = fragments
        .into_iter()
        .map(move |mut f| {
            if f.id == 0 {
                f.id = fragment_id;
                fragment_id += 1;
            }
            f
        })
        .collect::<Vec<_>>();

    let manifest = Manifest::new(
        lance_schema,
        Arc::new(fragments),
        DataStorageFormat::default(),
        Default::default(),
    );

    let version = 1;
    let inverted_version = u64::MAX - version;
    let file_name = format!("{inverted_version:020}.manifest");

    let (object_store, base_path) = LanceObjectStore::from_uri_and_params(
        Arc::new(ObjectStoreRegistry::default()),
        target_dataset_path,
        &store_params,
    )
    .await?;
    let path = base_path.child("_versions").child(file_name);

    let mut object_writer = ObjectWriter::new(&object_store, &path).await?;
    let pos = object_writer.write_struct(&manifest).await?;
    object_writer
        .write_magics(pos, MAJOR_VERSION, MINOR_VERSION, MAGIC)
        .await?;
    let _res = object_writer.shutdown().await?;

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
    output_blocks: Option<VecDeque<DataBlock>>,
    aggregated_summary: DataSummary,
    detailed_output: bool,
    committed: bool,
}

impl LanceDatasetCommitter {
    pub fn try_create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        info: CopyIntoLocationInfo,
        target_accessor: Operator,
        target_dataset_path: String,
        schema: TableSchemaRef,
        fragment_state: Arc<SharedFragmentState>,
    ) -> Result<ProcessorPtr> {
        let target_store_params = FragmentWriterParams::build_store_params(
            target_accessor,
            target_dataset_path.as_str(),
        )?;

        Ok(ProcessorPtr::create(Box::new(Self {
            input,
            output,
            target_dataset_path,
            target_store_params,
            schema,
            fragment_state,
            input_data: None,
            output_blocks: None,
            aggregated_summary: DataSummary::default(),
            detailed_output: info.options.detailed_output,
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
            let summary = DataSummary::from_block(&block);
            self.aggregated_summary.add(&summary);
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
        .await
        .map_err(|e| ErrorCode::StorageOther(format!("fail to commit lance dataset: {:?}", e)))?;

        if self.aggregated_summary.is_empty() {
            self.output_blocks = None;
        } else {
            let mut unload_output = UnloadOutput::create(self.detailed_output);
            unload_output.add_file(&self.target_dataset_path, self.aggregated_summary);
            self.output_blocks = Some(unload_output.to_block_partial().into());
        }
        self.committed = true;
        Ok(())
    }
}
