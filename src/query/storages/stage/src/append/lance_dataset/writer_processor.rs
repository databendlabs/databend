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

use arrow_array::Array;
use arrow_array::FixedSizeListArray;
use arrow_array::LargeListArray;
use arrow_array::ListArray;
use arrow_array::MapArray;
use arrow_array::RecordBatch;
use arrow_array::RecordBatchOptions;
use arrow_array::StringArray;
use arrow_array::cast::AsArray;
use arrow_schema::DataType as ArrowDataType;
use arrow_schema::Field;
use arrow_schema::FieldRef;
use arrow_schema::Fields;
use arrow_schema::Schema as ArrowSchema;
use async_trait::async_trait;
use databend_common_base::runtime::GlobalIORuntime;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::TableSchemaRef;
use databend_common_pipeline::core::Event;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline::core::ProcessorPtr;
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

use crate::append::column_based::block_batch::BlockBatch;
use crate::append::output::DataSummary;

const STAGING_MOVE_CONCURRENCY: usize = 8;
const STAGING_MOVE_CHUNK_SIZE: u64 = 8 * 1024 * 1024;

pub(crate) fn lance_compatible_arrow_schema(schema: &ArrowSchema) -> ArrowSchema {
    fn visit_field(field: &FieldRef) -> FieldRef {
        Arc::new(
            Field::new(
                field.name(),
                visit_type(field.data_type()),
                field.is_nullable(),
            )
            .with_metadata(field.metadata().clone()),
        )
    }

    fn visit_type(ty: &ArrowDataType) -> ArrowDataType {
        match ty {
            ArrowDataType::Utf8View => ArrowDataType::Utf8,
            ArrowDataType::List(field) => ArrowDataType::List(visit_field(field)),
            ArrowDataType::ListView(field) => ArrowDataType::ListView(visit_field(field)),
            ArrowDataType::FixedSizeList(field, len) => {
                ArrowDataType::FixedSizeList(visit_field(field), *len)
            }
            ArrowDataType::LargeList(field) => ArrowDataType::LargeList(visit_field(field)),
            ArrowDataType::LargeListView(field) => ArrowDataType::LargeListView(visit_field(field)),
            ArrowDataType::Struct(fields) => {
                let visited_fields = fields.iter().map(visit_field).collect::<Vec<_>>();
                ArrowDataType::Struct(Fields::from(visited_fields))
            }
            ArrowDataType::Union(fields, mode) => {
                let (ids, fields): (Vec<_>, Vec<_>) = fields
                    .iter()
                    .map(|(i, field)| (i, visit_field(field)))
                    .unzip();
                ArrowDataType::Union(
                    arrow_schema::UnionFields::try_new(ids, fields)
                        .expect("existing union fields should remain valid"),
                    *mode,
                )
            }
            ArrowDataType::Dictionary(key, value) => {
                ArrowDataType::Dictionary(Box::new(visit_type(key)), Box::new(visit_type(value)))
            }
            ArrowDataType::Map(field, ordered) => ArrowDataType::Map(visit_field(field), *ordered),
            ty => {
                debug_assert!(!ty.is_nested());
                ty.clone()
            }
        }
    }

    let fields = schema.fields().iter().map(visit_field).collect::<Vec<_>>();
    ArrowSchema::new(fields).with_metadata(schema.metadata().clone())
}

fn lance_compatible_array(array: Arc<dyn Array>, data_type: &ArrowDataType) -> Arc<dyn Array> {
    match data_type {
        ArrowDataType::Utf8 => {
            if matches!(array.data_type(), ArrowDataType::Utf8View) {
                Arc::new(StringArray::from_iter(array.as_string_view().iter())) as _
            } else {
                array
            }
        }
        ArrowDataType::Struct(fields) => {
            let array = array.as_struct();
            let inner_arrays = array
                .columns()
                .iter()
                .zip(fields.iter())
                .map(|(column, field)| lance_compatible_array(column.clone(), field.data_type()))
                .collect::<Vec<_>>();
            Arc::new(arrow_array::StructArray::new(
                fields.clone(),
                inner_arrays,
                array.nulls().cloned(),
            )) as _
        }
        ArrowDataType::List(field) => {
            let array = array.as_list::<i32>();
            let values = lance_compatible_array(array.values().clone(), field.data_type());
            Arc::new(ListArray::new(
                field.clone(),
                array.offsets().clone(),
                values,
                array.nulls().cloned(),
            )) as _
        }
        ArrowDataType::LargeList(field) => {
            let array = array.as_list::<i64>();
            let values = lance_compatible_array(array.values().clone(), field.data_type());
            Arc::new(LargeListArray::new(
                field.clone(),
                array.offsets().clone(),
                values,
                array.nulls().cloned(),
            )) as _
        }
        ArrowDataType::FixedSizeList(field, size) => {
            let array = array.as_fixed_size_list();
            let values = lance_compatible_array(array.values().clone(), field.data_type());
            Arc::new(FixedSizeListArray::new(
                field.clone(),
                *size,
                values,
                array.nulls().cloned(),
            )) as _
        }
        ArrowDataType::Map(field, ordered) => {
            let array = array.as_map();
            let entries =
                lance_compatible_array(Arc::new(array.entries().clone()) as _, field.data_type());
            Arc::new(MapArray::new(
                field.clone(),
                array.offsets().clone(),
                entries.as_struct().clone(),
                array.nulls().cloned(),
                *ordered,
            )) as _
        }
        _ => array,
    }
}

pub(crate) fn lance_compatible_record_batch(
    batch: RecordBatch,
    arrow_schema: Arc<ArrowSchema>,
) -> Result<RecordBatch> {
    if arrow_schema.fields().is_empty() {
        return Ok(RecordBatch::try_new_with_options(
            arrow_schema,
            vec![],
            &RecordBatchOptions::default().with_row_count(Some(batch.num_rows())),
        )?);
    }

    let arrays = batch
        .columns()
        .iter()
        .zip(arrow_schema.fields())
        .map(|(array, field)| lance_compatible_array(array.clone(), field.data_type()))
        .collect::<Vec<_>>();

    Ok(RecordBatch::try_new(arrow_schema, arrays)?)
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

    input_data: Vec<DataBlock>,

    file_to_move: Option<PreparedMoveTask>,

    final_output_state: FinalOutputState,

    // Aggregated write stats across all completed flush+move operations.
    written_summary: DataSummary,

    params: Arc<FragmentWriterParams>,
    fragment_state: Arc<SharedFragmentState>,
}

pub(crate) struct FragmentWriterParams {
    schema: TableSchemaRef,
    arrow_schema: Arc<ArrowSchema>,

    target_accessor: Operator,
    target_dataset_path: String,

    staging_accessor: Operator,
    staging_dataset_path: String,

    lance_schema: LanceSchema,

    object_store: Arc<LanceObjectStore>,
    base_path: object_store::path::Path,
}

impl FragmentWriterParams {
    pub fn try_create(
        schema: TableSchemaRef,
        target_accessor: Operator,
        target_dataset_path: String,
        staging_accessor: Operator,
        staging_dataset_path: String,
    ) -> Result<Self> {
        let arrow_schema = Arc::new(lance_compatible_arrow_schema(&ArrowSchema::from(
            schema.as_ref(),
        )));
        let lance_schema = LanceSchema::try_from(arrow_schema.as_ref()).map_err(|err| {
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

        Ok(Self {
            schema,
            arrow_schema,
            target_accessor,
            target_dataset_path,
            staging_accessor,
            staging_dataset_path,
            lance_schema,
            object_store,
            base_path,
        })
    }

    pub(crate) fn build_store_params(
        data_accessor: Operator,
        dataset_path: &str,
    ) -> Result<ObjectStoreParams> {
        let scheme = data_accessor.info().scheme().to_string();
        let object_store: Arc<dyn ObjectStore> = Arc::new(OpendalStore::new(data_accessor));

        let mut root = PathBuf::from("/");
        let normalized = dataset_path.trim_matches('/');
        if !normalized.is_empty() {
            root.push(normalized);
        }
        let mut base_url = Url::parse(&format!("{scheme}:///")).map_err(|err| {
            ErrorCode::Internal(format!("invalid lance object store scheme '{scheme}': {err}"))
        })?;
        base_url.set_path(root.to_string_lossy().as_ref());

        #[allow(deprecated)]
        let store_params = ObjectStoreParams {
            object_store: Some((object_store, base_url)),
            ..Default::default()
        };
        Ok(store_params)
    }
}

impl LanceDatasetWriter {
    pub fn try_create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        params: Arc<FragmentWriterParams>,
        fragment_state: Arc<SharedFragmentState>,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Box::new(LanceDatasetWriter {
            input,
            output,
            params,
            fragment_state,
            input_data: Vec::new(),
            written_summary: DataSummary::default(),
            file_to_move: None,
            final_output_state: FinalOutputState::Unprepared,
        })))
    }

    fn build_final_output_block(&self) -> Option<DataBlock> {
        if self.written_summary.is_empty() {
            return None;
        }

        Some(self.written_summary.to_block())
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

    async fn append_to_staging_dataset(
        &self,
        batches: Vec<RecordBatch>,
        mut summary: DataSummary,
    ) -> Result<PreparedMoveTask> {
        let mut fragments = Vec::new();

        let mut fragment = Fragment::new(0);
        let data_file_key = uuid::Uuid::new_v4().to_string();
        let filename = format!("{}.lance", data_file_key);
        // let has_blob_v2 = schema_has_blob_v2(&schema);
        let full_path = self.params.base_path.child("data").child(filename.clone());
        let obj_writer = self
            .params
            .object_store
            .create(&full_path)
            .await
            .map_err(|err| {
                ErrorCode::StorageOther(format!("create staging lance data file failed: {err}"))
            })?;
        let mut file_writer = lance_file::writer::FileWriter::try_new(
            obj_writer,
            self.params.lance_schema.clone(),
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
        for batch in &batches {
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
            .params
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
                let source_accessor = self.params.staging_accessor.clone();
                let target_accessor = self.params.target_accessor.clone();
                let source_dataset_path = self.params.staging_dataset_path.clone();
                let target_dataset_path = self.params.target_dataset_path.clone();

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
        } else if !self.input_data.is_empty() {
            self.input.set_not_need_data();
            Ok(Event::Sync)
        } else if self.input.is_finished() {
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
        } else if self.input.has_data() {
            let block = self.input.pull_data().unwrap()?;
            if block.get_meta().is_some() {
                let block_meta = block.get_owned_meta().unwrap();
                let block_batch = BlockBatch::downcast_from(block_meta).unwrap();
                for b in block_batch.blocks {
                    self.input_data.push(b);
                }
            } else {
                self.input_data.push(block);
            };
            self.input.set_not_need_data();
            Ok(Event::Sync)
        } else {
            self.input.set_need_data();
            Ok(Event::NeedData)
        }
    }

    fn process(&mut self) -> Result<()> {
        if self.input_data.is_empty() {
            return Ok(());
        }
        let mut batches = Vec::new();

        let mut summary = DataSummary::default();
        for block in std::mem::take(&mut self.input_data) {
            summary.row_counts += block.num_rows();
            summary.input_bytes = summary.input_bytes.saturating_add(block.memory_size());
            let batch = block.to_record_batch(self.params.schema.as_ref())?;
            let batch = lance_compatible_record_batch(batch, self.params.arrow_schema.clone())?;
            batches.push(batch)
        }
        if summary.row_counts == 0 {
            return Ok(());
        }

        let prepared = Self::with_thread_local_runtime(|rt| {
            rt.block_on(self.append_to_staging_dataset(batches, summary))
        })?;

        if prepared.fragments.is_empty() {
            self.written_summary.add(&prepared.summary);
        } else {
            self.file_to_move = Some(prepared);
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::StringViewArray;
    use arrow_schema::Field;
    use lance_io::object_store::ObjectStore as LanceObjectStore;
    use lance_io::object_store::ObjectStoreRegistry;
    use object_store::path::Path;
    use opendal::Operator;
    use opendal::services::Memory;

    use super::*;

    #[test]
    fn test_lance_compatible_arrow_schema_converts_nested_utf8_view() {
        let inner = Arc::new(Field::new("inner", ArrowDataType::Utf8View, true));
        let schema = ArrowSchema::new(vec![Field::new(
            "nested",
            ArrowDataType::Struct(Fields::from(vec![inner])),
            true,
        )]);

        let normalized = lance_compatible_arrow_schema(&schema);
        let ArrowDataType::Struct(fields) = normalized.field(0).data_type() else {
            panic!("expected struct field")
        };

        assert_eq!(fields[0].data_type(), &ArrowDataType::Utf8);
    }

    #[test]
    fn test_lance_compatible_record_batch_converts_utf8_view_arrays() -> Result<()> {
        let source_field = Arc::new(Field::new("inner", ArrowDataType::Utf8View, true));
        let source_schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "nested",
            ArrowDataType::Struct(Fields::from(vec![source_field.clone()])),
            true,
        )]));
        let target_field = Arc::new(Field::new("inner", ArrowDataType::Utf8, true));
        let target_schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "nested",
            ArrowDataType::Struct(Fields::from(vec![target_field.clone()])),
            true,
        )]));

        let batch = RecordBatch::try_new(source_schema, vec![Arc::new(
            arrow_array::StructArray::new(
                Fields::from(vec![source_field]),
                vec![Arc::new(StringViewArray::from(vec![Some("abc"), None]))],
                None,
            ),
        )])?;

        let normalized = lance_compatible_record_batch(batch, target_schema)?;
        let nested = normalized.column(0).as_struct();

        assert_eq!(nested.column(0).data_type(), &ArrowDataType::Utf8);
        let strings = nested.column(0).as_string::<i32>();
        assert_eq!(strings.value(0), "abc");
        assert!(strings.is_null(1));

        Ok(())
    }

    #[test]
    fn test_build_store_params_preserves_accessor_scheme() {
        let accessor = Operator::new(Memory::default()).unwrap().finish();
        let params = FragmentWriterParams::build_store_params(accessor, "tmp").unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let (store, base_path) = runtime
            .block_on(async {
                LanceObjectStore::from_uri_and_params(
                    Arc::new(ObjectStoreRegistry::default()),
                    "tmp",
                    &params,
                )
                .await
            })
            .unwrap();

        assert!(!store.is_local());
        assert_eq!(base_path, Path::parse("/tmp").unwrap());
    }
}
