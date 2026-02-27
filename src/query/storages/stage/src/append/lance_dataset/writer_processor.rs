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
use std::cell::OnceCell;
use std::mem;
use std::num::NonZeroU64;
use std::path::PathBuf;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::Schema as ArrowSchema;
use async_trait::async_trait;
use databend_common_base::runtime::GlobalIORuntime;
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
use futures::StreamExt;
use lance_core::datatypes::Schema as LanceSchema;
use lance_file::writer::FileWriterOptions;
use lance_io::object_store::ObjectStore as LanceObjectStore;
use lance_io::object_store::ObjectStoreParams;
use lance_io::object_store::ObjectStoreRegistry;
use lance_table::format::DataFile;
use lance_table::format::Fragment;
use log::info;
use object_store::ObjectStore;
use object_store_opendal::OpendalStore;
use opendal::Operator;
use tokio::sync::Mutex;
use url::Url;

use crate::append::output::DataSummary;

const STAGING_MOVE_CONCURRENCY: usize = 8;
const STAGING_MOVE_CHUNK_SIZE: u64 = 8 * 1024 * 1024;

struct DatasetWriteTask {
    batches: Vec<RecordBatch>,
    summary: DataSummary,
}

struct PreparedMoveTask {
    fragments: Vec<Fragment>,
    summary: DataSummary,
}

enum FinalOutputState {
    Unprepared,
    Ready(DataBlock),
    Done,
}

pub(crate) struct SharedFragmentState {
    fragments: Mutex<Vec<Fragment>>,
}

impl SharedFragmentState {
    pub(crate) fn new() -> Self {
        Self {
            fragments: Mutex::new(Vec::new()),
        }
    }

    pub(crate) async fn add_fragments(&self, fragments: Vec<Fragment>) {
        if fragments.is_empty() {
            return;
        }

        let mut staged = self.fragments.lock().await;
        staged.extend(fragments);
    }

    pub(crate) async fn take_fragments(&self) -> Vec<Fragment> {
        let mut staged = self.fragments.lock().await;
        mem::take(&mut *staged)
    }
}

pub struct LanceDatasetWriter {
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    schema: TableSchemaRef,

    target_accessor: Operator,
    target_dataset_path: String,

    staging_accessor: Operator,
    staging_dataset_path: String,
    fragment_state: Arc<SharedFragmentState>,

    input_data: Option<DataBlock>,

    // Accumulate small batches to avoid writing tiny fragments.
    batches: Vec<RecordBatch>,
    // Number of rows currently buffered in `batches` and not flushed yet.
    // Reset to 0 in `build_write_task` after creating a flush task.
    batches_row_counts: usize,
    // Number of bytes currently buffered in `batches` and not flushed yet.
    // Used for flush-threshold checks, then reset in `build_write_task`.
    batches_input_bytes: usize,

    // Aggregated write stats across all completed flush+move operations.
    written_summary: DataSummary,

    lance_schema: LanceSchema,
    file_to_move: Option<PreparedMoveTask>,

    final_output_state: FinalOutputState,

    max_bytes_per_file: usize,
    max_rows_per_file: usize,
    object_store: Arc<LanceObjectStore>,
    base_path: object_store::path::Path,
}

impl LanceDatasetWriter {
    #[allow(clippy::too_many_arguments)]
    pub fn try_create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        info: CopyIntoLocationInfo,
        schema: TableSchemaRef,
        target_accessor: Operator,
        target_dataset_path: String,
        staging_accessor: Operator,
        staging_dataset_path: String,
        fragment_state: Arc<SharedFragmentState>,
    ) -> Result<ProcessorPtr> {
        let max_bytes_per_file = if info.options.single {
            usize::MAX
        } else if info.options.max_file_size == 0 {
            128 * 1024 * 1024
        } else {
            info.options.max_file_size
        };
        let arrow_schema = ArrowSchema::from(schema.as_ref());
        let lance_schema = LanceSchema::try_from(&arrow_schema).map_err(|err| {
            ErrorCode::StorageOther(format!("lance schema conversion failed: {err}"))
        })?;
        let staging_store_params =
            Self::build_store_params(staging_accessor.clone(), staging_dataset_path.as_str())?;

        let staging_dataset_path_clone = staging_dataset_path.clone();
        let (object_store, base_path) = GlobalIORuntime::instance().block_on(async move {
            LanceObjectStore::from_uri_and_params(
                Arc::new(ObjectStoreRegistry::default()),
                &staging_dataset_path_clone,
                &staging_store_params,
            )
            .await
            .map_err(|e| {
                ErrorCode::StorageOther(format!("lance object store creation failed: {e}"))
            })
        })?;

        Ok(ProcessorPtr::create(Box::new(LanceDatasetWriter {
            input,
            output,
            schema,
            target_accessor,
            target_dataset_path,
            staging_accessor,
            staging_dataset_path,
            fragment_state,
            input_data: None,
            batches: Vec::new(),
            batches_row_counts: 0,
            batches_input_bytes: 0,
            written_summary: DataSummary::default(),
            lance_schema,
            file_to_move: None,
            final_output_state: FinalOutputState::Unprepared,
            max_bytes_per_file,
            max_rows_per_file: 1000,
            object_store,
            base_path,
        })))
    }

    fn build_final_output_block(&self) -> Option<DataBlock> {
        if self.written_summary.is_empty() {
            return None;
        }

        Some(self.written_summary.to_block())
    }

    fn need_flush(&self) -> bool {
        self.batches_input_bytes >= self.max_bytes_per_file
            || self.batches_row_counts >= self.max_rows_per_file
    }

    fn flush_pending_batches_sync(&mut self) -> Result<()> {
        if self.batches_row_counts == 0 || self.batches.is_empty() {
            return Ok(());
        }

        let summary = DataSummary {
            row_counts: self.batches_row_counts,
            input_bytes: self.batches_input_bytes,
            output_bytes: 0,
        };
        self.batches_row_counts = 0;
        self.batches_input_bytes = 0;

        let task = DatasetWriteTask {
            batches: mem::take(&mut self.batches),
            summary,
        };

        let prepared = Self::with_thread_local_runtime(|rt| {
            rt.block_on(self.append_to_staging_dataset(task))
        })?;

        if prepared.fragments.is_empty() {
            self.written_summary.add(&prepared.summary);
        } else {
            self.file_to_move = Some(prepared);
        }

        Ok(())
    }

    pub(crate) fn build_store_params(
        data_accessor: Operator,
        dataset_path: &str,
    ) -> Result<ObjectStoreParams> {
        let object_store: Arc<dyn ObjectStore> = Arc::new(OpendalStore::new(data_accessor));

        let mut root = PathBuf::from("/");
        let normalized = dataset_path.trim_matches('/');
        if !normalized.is_empty() {
            root.push(normalized);
        }
        let base_url = Url::from_directory_path(root).map_err(|_| {
            ErrorCode::Internal("invalid base url for lance object store".to_string())
        })?;

        #[allow(deprecated)]
        let store_params = ObjectStoreParams {
            object_store: Some((object_store, base_url)),
            ..Default::default()
        };
        Ok(store_params)
    }

    fn data_file_path(dataset_path: &str, relative_path: &str) -> String {
        let base = dataset_path.trim_end_matches('/');
        let relative = relative_path.trim_start_matches('/');
        if base.is_empty() {
            format!("data/{relative}")
        } else {
            format!("{base}/data/{relative}")
        }
    }

    #[allow(clippy::disallowed_methods)]
    fn with_thread_local_runtime<T, F>(f: F) -> Result<T>
    where F: FnOnce(&tokio::runtime::Runtime) -> Result<T> {
        thread_local! {
            static LANCE_WRITER_RUNTIME: OnceCell<tokio::runtime::Runtime> = const { OnceCell::new() };
        }

        LANCE_WRITER_RUNTIME
            .try_with(|cell| {
                let runtime = if let Some(runtime) = cell.get() {
                    runtime
                } else {
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .enable_time()
                        .enable_io()
                        .build()
                        .map_err(|err| {
                            ErrorCode::Internal(format!("init lance writer runtime failed: {err}"))
                        })?;
                    let _ = cell.set(runtime);
                    cell.get().ok_or_else(|| {
                        ErrorCode::Internal(
                            "lance writer runtime is unavailable after initialization".to_string(),
                        )
                    })?
                };

                f(runtime)
            })
            .map_err(|err| {
                ErrorCode::Internal(format!("access lance writer runtime failed: {err}"))
            })?
    }

    async fn append_to_staging_dataset(&self, task: DatasetWriteTask) -> Result<PreparedMoveTask> {
        let mut fragments = Vec::new();

        let mut fragment = Fragment::new(0);
        let data_file_key = uuid::Uuid::new_v4().to_string();
        let filename = format!("{}.lance", data_file_key);
        // let has_blob_v2 = schema_has_blob_v2(&schema);
        let full_path = self.base_path.child("data").child(filename.clone());
        let obj_writer = self.object_store.create(&full_path).await.map_err(|err| {
            ErrorCode::StorageOther(format!("create staging lance data file failed: {err}"))
        })?;
        let mut file_writer = lance_file::writer::FileWriter::try_new(
            obj_writer,
            self.lance_schema.clone(),
            FileWriterOptions::default(),
        )
        .map_err(|err| {
            ErrorCode::StorageOther(format!(
                "create lance file writer for staging failed: {err}"
            ))
        })?;

        // let preprocessor = if schema_has_blob_v2(schema) {
        //     Some(BlobPreprocessor::new(
        //         object_store.clone(),
        //         data_dir.clone(),
        //         data_file_key.clone(),
        //     ))
        // } else {
        //     None
        // };
        for batch in &task.batches {
            file_writer.write_batch(batch).await.map_err(|err| {
                ErrorCode::StorageOther(format!(
                    "write batch into lance staging file failed: {err}"
                ))
            })?;
        }
        let num_rows = file_writer.finish().await.map_err(|err| {
            ErrorCode::StorageOther(format!("finalize lance staging file failed: {err}"))
        })?;
        fragment.physical_rows = Some(num_rows as usize);

        let file_meta = self
            .object_store
            .inner
            .head(&full_path)
            .await
            .map_err(|err| {
                ErrorCode::StorageOther(format!("fetch staging lance file metadata failed: {err}"))
            })?;

        let field_ids = file_writer
            .field_id_to_column_indices()
            .iter()
            .map(|(field_id, _)| *field_id as i32)
            .collect::<Vec<_>>();
        let column_indices = file_writer
            .field_id_to_column_indices()
            .iter()
            .map(|(_, column_index)| *column_index as i32)
            .collect::<Vec<_>>();
        let (major, minor) = file_writer.version().to_numbers();
        let data_file = DataFile::new(
            filename,
            field_ids,
            column_indices,
            major,
            minor,
            NonZeroU64::new(file_meta.size),
            None,
        );
        fragment.files.push(data_file);

        let mut summary = task.summary;
        summary.output_bytes = file_meta.size as usize;
        fragments.push(fragment);

        Ok(PreparedMoveTask { fragments, summary })
    }

    async fn move_written_lance_files(
        &self,
        task: PreparedMoveTask,
    ) -> Result<(DataSummary, Vec<Fragment>)> {
        let encoded_file_paths = task
            .fragments
            .iter()
            .flat_map(|fragment| fragment.files.iter())
            .map(|file| file.path.clone())
            .collect::<Vec<_>>();

        let move_results =
            futures::stream::iter(encoded_file_paths.into_iter().map(|relative_path| {
                let source_accessor = self.staging_accessor.clone();
                let target_accessor = self.target_accessor.clone();
                let source_dataset_path = self.staging_dataset_path.clone();
                let target_dataset_path = self.target_dataset_path.clone();

                async move {
                    let source =
                        Self::data_file_path(source_dataset_path.as_str(), relative_path.as_str());
                    let metadata = source_accessor.stat(source.as_str()).await.map_err(|err| {
                        ErrorCode::StorageOther(format!(
                            "cannot locate staging lance data file for '{}', expected='{}': {}",
                            relative_path, source, err
                        ))
                    })?;

                    let destination =
                        Self::data_file_path(target_dataset_path.as_str(), relative_path.as_str());

                    info!(
                        "move lance data file from staging to target: {} -> {}",
                        source, destination
                    );

                    let mut writer = target_accessor
                        .writer_with(destination.as_str())
                        .chunk(STAGING_MOVE_CHUNK_SIZE as usize)
                        .await?;
                    let mut offset = 0_u64;
                    let total_size = metadata.content_length();
                    while offset < total_size {
                        let end = (offset + STAGING_MOVE_CHUNK_SIZE).min(total_size);
                        let chunk = source_accessor
                            .read_with(source.as_str())
                            .range(offset..end)
                            .await?;
                        writer.write(chunk).await?;
                        offset = end;
                    }
                    writer.close().await?;
                    source_accessor.delete(source.as_str()).await?;

                    Ok::<(), ErrorCode>(())
                }
            }))
            .buffer_unordered(STAGING_MOVE_CONCURRENCY)
            .collect::<Vec<_>>()
            .await;

        for result in move_results {
            result?;
        }

        Ok((task.summary, task.fragments))
    }
}

#[async_trait]
impl Processor for LanceDatasetWriter {
    fn name(&self) -> String {
        "LanceDatasetWriter".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.output.is_finished() {
            self.input.finish();
            Ok(Event::Finished)
        } else if self.file_to_move.is_some() {
            self.input.set_not_need_data();
            Ok(Event::Async)
        } else if self.input_data.is_some() {
            self.input.set_not_need_data();
            Ok(Event::Sync)
        } else if self.input.is_finished() {
            if self.batches_row_counts > 0 {
                self.input.set_not_need_data();
                Ok(Event::Sync)
            } else {
                if matches!(self.final_output_state, FinalOutputState::Unprepared) {
                    self.final_output_state = match self.build_final_output_block() {
                        Some(block) => FinalOutputState::Ready(block),
                        None => FinalOutputState::Done,
                    };

                    if matches!(self.final_output_state, FinalOutputState::Done) {
                        self.output.finish();
                        return Ok(Event::Finished);
                    }
                }

                if self.output.can_push() {
                    let output_state =
                        mem::replace(&mut self.final_output_state, FinalOutputState::Done);
                    match output_state {
                        FinalOutputState::Ready(block) => {
                            self.output.push_data(Ok(block));
                            Ok(Event::NeedConsume)
                        }
                        FinalOutputState::Unprepared | FinalOutputState::Done => {
                            self.output.finish();
                            Ok(Event::Finished)
                        }
                    }
                } else {
                    Ok(Event::NeedConsume)
                }
            }
        } else if self.input.has_data() {
            self.input_data = Some(self.input.pull_data().unwrap()?);
            self.input.set_not_need_data();
            Ok(Event::Sync)
        } else {
            self.input.set_need_data();
            Ok(Event::NeedData)
        }
    }

    fn process(&mut self) -> Result<()> {
        if let Some(block) = self.input_data.take() {
            self.batches_row_counts += block.num_rows();
            self.batches_input_bytes = self.batches_input_bytes.saturating_add(block.memory_size());
            let batch = block.to_record_batch(self.schema.as_ref())?;
            self.batches.push(batch);
            if self.need_flush() {
                self.flush_pending_batches_sync()?;
            }
            return Ok(());
        }

        if self.input.is_finished() && self.batches_row_counts > 0 {
            self.flush_pending_batches_sync()?;
        }

        Ok(())
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        if let Some(task) = self.file_to_move.take() {
            let (summary, fragments) = self.move_written_lance_files(task).await?;
            self.fragment_state.add_fragments(fragments).await;
            self.written_summary.add(&summary);
            return Ok(());
        }

        Err(ErrorCode::Internal(
            "unexpected async state in LanceDatasetWriter".to_string(),
        ))
    }
}
