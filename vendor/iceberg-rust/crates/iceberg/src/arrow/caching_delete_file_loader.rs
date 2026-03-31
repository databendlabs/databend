// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::{HashMap, HashSet};
use std::ops::Not;
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, Int64Array, StringArray, StructArray};
use futures::{StreamExt, TryStreamExt};
use tokio::sync::oneshot::{Receiver, channel};

use super::delete_filter::{DeleteFilter, PosDelLoadAction};
use crate::arrow::delete_file_loader::BasicDeleteFileLoader;
use crate::arrow::{arrow_primitive_to_literal, arrow_schema_to_schema};
use crate::delete_vector::DeleteVector;
use crate::expr::Predicate::AlwaysTrue;
use crate::expr::{Predicate, Reference};
use crate::io::FileIO;
use crate::scan::{ArrowRecordBatchStream, FileScanTaskDeleteFile};
use crate::spec::{
    DataContentType, Datum, ListType, MapType, NestedField, NestedFieldRef, PartnerAccessor,
    PrimitiveType, Schema, SchemaRef, SchemaWithPartnerVisitor, StructType, Type,
    visit_schema_with_partner,
};
use crate::{Error, ErrorKind, Result};

#[derive(Clone, Debug)]
pub(crate) struct CachingDeleteFileLoader {
    basic_delete_file_loader: BasicDeleteFileLoader,
    concurrency_limit_data_files: usize,
    /// Shared filter state to allow caching loaded deletes across multiple
    /// calls to `load_deletes` (e.g., across multiple file scan tasks).
    delete_filter: DeleteFilter,
}

// Intermediate context during processing of a delete file task.
enum DeleteFileContext {
    // TODO: Delete Vector loader from Puffin files
    ExistingEqDel,
    ExistingPosDel,
    PosDels {
        file_path: String,
        stream: ArrowRecordBatchStream,
    },
    FreshEqDel {
        batch_stream: ArrowRecordBatchStream,
        equality_ids: HashSet<i32>,
        sender: tokio::sync::oneshot::Sender<Predicate>,
    },
}

// Final result of the processing of a delete file task before
// results are fully merged into the DeleteFileManager's state
enum ParsedDeleteFileContext {
    DelVecs {
        file_path: String,
        results: HashMap<String, DeleteVector>,
    },
    EqDel,
    ExistingPosDel,
}

#[allow(unused_variables)]
impl CachingDeleteFileLoader {
    pub(crate) fn new(file_io: FileIO, concurrency_limit_data_files: usize) -> Self {
        CachingDeleteFileLoader {
            basic_delete_file_loader: BasicDeleteFileLoader::new(file_io),
            concurrency_limit_data_files,
            delete_filter: DeleteFilter::default(),
        }
    }

    /// Initiates loading of all deletes for all the specified tasks
    ///
    /// Returned future completes once all positional deletes and delete vectors
    /// have loaded. EQ deletes are not waited for in this method but the returned
    /// DeleteFilter will await their loading when queried for them.
    ///
    ///  * Create a single stream of all delete file tasks irrespective of type,
    ///    so that we can respect the combined concurrency limit
    ///  * We then process each in two phases: load and parse.
    ///  * for positional deletes the load phase instantiates an ArrowRecordBatchStream to
    ///    stream the file contents out
    ///  * for eq deletes, we first check if the EQ delete is already loaded or being loaded by
    ///    another concurrently processing data file scan task. If it is, we skip it.
    ///    If not, the DeleteFilter is updated to contain a notifier to prevent other data file
    ///    tasks from starting to load the same equality delete file. We spawn a task to load
    ///    the EQ delete's record batch stream, convert it to a predicate, update the delete filter,
    ///    and notify any task that was waiting for it.
    ///  * When this gets updated to add support for delete vectors, the load phase will return
    ///    a PuffinReader for them.
    ///  * The parse phase parses each record batch stream according to its associated data type.
    ///    The result of this is a map of data file paths to delete vectors for the positional
    ///    delete tasks (and in future for the delete vector tasks). For equality delete
    ///    file tasks, this results in an unbound Predicate.
    ///  * The unbound Predicates resulting from equality deletes are sent to their associated oneshot
    ///    channel to store them in the right place in the delete file managers state.
    ///  * The results of all of these futures are awaited on in parallel with the specified
    ///    level of concurrency and collected into a vec. We then combine all the delete
    ///    vector maps that resulted from any positional delete or delete vector files into a
    ///    single map and persist it in the state.
    ///
    ///
    ///  Conceptually, the data flow is like this:
    /// ```none
    ///                                          FileScanTaskDeleteFile
    ///                                                     |
    ///                                             Skip Started EQ Deletes
    ///                                                     |
    ///                                                     |
    ///                                       [load recordbatch stream / puffin]
    ///                                             DeleteFileContext
    ///                                                     |
    ///                                                     |
    ///                       +-----------------------------+--------------------------+
    ///                     Pos Del           Del Vec (Not yet Implemented)         EQ Del
    ///                       |                             |                          |
    ///              [parse pos del stream]         [parse del vec puffin]       [parse eq del]
    ///          HashMap<String, RoaringTreeMap> HashMap<String, RoaringTreeMap>   (Predicate, Sender)
    ///                       |                             |                          |
    ///                       |                             |                 [persist to state]
    ///                       |                             |                          ()
    ///                       |                             |                          |
    ///                       +-----------------------------+--------------------------+
    ///                                                     |
    ///                                             [buffer unordered]
    ///                                                     |
    ///                                            [combine del vectors]
    ///                                        HashMap<String, RoaringTreeMap>
    ///                                                     |
    ///                                        [persist del vectors to state]
    ///                                                    ()
    ///                                                    |
    ///                                                    |
    ///                                                 [join!]
    /// ```
    pub(crate) fn load_deletes(
        &self,
        delete_file_entries: &[FileScanTaskDeleteFile],
        schema: SchemaRef,
    ) -> Receiver<Result<DeleteFilter>> {
        let (tx, rx) = channel();

        let stream_items = delete_file_entries
            .iter()
            .map(|t| {
                (
                    t.clone(),
                    self.basic_delete_file_loader.clone(),
                    self.delete_filter.clone(),
                    schema.clone(),
                )
            })
            .collect::<Vec<_>>();
        let task_stream = futures::stream::iter(stream_items);

        let del_filter = self.delete_filter.clone();
        let concurrency_limit_data_files = self.concurrency_limit_data_files;
        let basic_delete_file_loader = self.basic_delete_file_loader.clone();
        crate::runtime::spawn(async move {
            let result = async move {
                let mut del_filter = del_filter;
                let basic_delete_file_loader = basic_delete_file_loader.clone();

                let mut results_stream = task_stream
                    .map(move |(task, file_io, del_filter, schema)| {
                        let basic_delete_file_loader = basic_delete_file_loader.clone();
                        async move {
                            Self::load_file_for_task(
                                &task,
                                basic_delete_file_loader.clone(),
                                del_filter,
                                schema,
                            )
                            .await
                        }
                    })
                    .map(move |ctx| {
                        Ok(async { Self::parse_file_content_for_task(ctx.await?).await })
                    })
                    .try_buffer_unordered(concurrency_limit_data_files);

                while let Some(item) = results_stream.next().await {
                    let item = item?;
                    if let ParsedDeleteFileContext::DelVecs { file_path, results } = item {
                        for (data_file_path, delete_vector) in results.into_iter() {
                            del_filter.upsert_delete_vector(data_file_path, delete_vector);
                        }
                        // Mark the positional delete file as fully loaded so waiters can proceed
                        del_filter.finish_pos_del_load(&file_path);
                    }
                }

                Ok(del_filter)
            }
            .await;

            let _ = tx.send(result);
        });

        rx
    }

    async fn load_file_for_task(
        task: &FileScanTaskDeleteFile,
        basic_delete_file_loader: BasicDeleteFileLoader,
        del_filter: DeleteFilter,
        schema: SchemaRef,
    ) -> Result<DeleteFileContext> {
        match task.file_type {
            DataContentType::PositionDeletes => {
                match del_filter.try_start_pos_del_load(&task.file_path) {
                    PosDelLoadAction::AlreadyLoaded => Ok(DeleteFileContext::ExistingPosDel),
                    PosDelLoadAction::WaitFor(notify) => {
                        // Positional deletes are accessed synchronously by ArrowReader.
                        // We must wait here to ensure the data is ready before returning,
                        // otherwise ArrowReader might get an empty/partial result.
                        notify.notified().await;
                        Ok(DeleteFileContext::ExistingPosDel)
                    }
                    PosDelLoadAction::Load => Ok(DeleteFileContext::PosDels {
                        file_path: task.file_path.clone(),
                        stream: basic_delete_file_loader
                            .parquet_to_batch_stream(&task.file_path)
                            .await?,
                    }),
                }
            }

            DataContentType::EqualityDeletes => {
                let Some(notify) = del_filter.try_start_eq_del_load(&task.file_path) else {
                    return Ok(DeleteFileContext::ExistingEqDel);
                };

                let (sender, receiver) = channel();
                del_filter.insert_equality_delete(&task.file_path, receiver);

                // Per the Iceberg spec, evolve schema for equality deletes but only for the
                // equality_ids columns, not all table columns.
                let equality_ids_vec = task.equality_ids.clone().unwrap();
                let evolved_stream = BasicDeleteFileLoader::evolve_schema(
                    basic_delete_file_loader
                        .parquet_to_batch_stream(&task.file_path)
                        .await?,
                    schema,
                    &equality_ids_vec,
                )
                .await?;

                Ok(DeleteFileContext::FreshEqDel {
                    batch_stream: evolved_stream,
                    sender,
                    equality_ids: HashSet::from_iter(equality_ids_vec),
                })
            }

            DataContentType::Data => Err(Error::new(
                ErrorKind::Unexpected,
                "tasks with files of type Data not expected here",
            )),
        }
    }

    async fn parse_file_content_for_task(
        ctx: DeleteFileContext,
    ) -> Result<ParsedDeleteFileContext> {
        match ctx {
            DeleteFileContext::ExistingEqDel => Ok(ParsedDeleteFileContext::EqDel),
            DeleteFileContext::ExistingPosDel => Ok(ParsedDeleteFileContext::ExistingPosDel),
            DeleteFileContext::PosDels { file_path, stream } => {
                let del_vecs = Self::parse_positional_deletes_record_batch_stream(stream).await?;
                Ok(ParsedDeleteFileContext::DelVecs {
                    file_path,
                    results: del_vecs,
                })
            }
            DeleteFileContext::FreshEqDel {
                sender,
                batch_stream,
                equality_ids,
            } => {
                let predicate =
                    Self::parse_equality_deletes_record_batch_stream(batch_stream, equality_ids)
                        .await?;

                sender
                    .send(predicate)
                    .map_err(|err| {
                        Error::new(
                            ErrorKind::Unexpected,
                            "Could not send eq delete predicate to state",
                        )
                    })
                    .map(|_| ParsedDeleteFileContext::EqDel)
            }
        }
    }

    /// Parses a record batch stream coming from positional delete files
    ///
    /// Returns a map of data file path to a delete vector
    async fn parse_positional_deletes_record_batch_stream(
        mut stream: ArrowRecordBatchStream,
    ) -> Result<HashMap<String, DeleteVector>> {
        let mut result: HashMap<String, DeleteVector> = HashMap::default();

        while let Some(batch) = stream.next().await {
            let batch = batch?;
            let schema = batch.schema();
            let columns = batch.columns();

            let Some(file_paths) = columns[0].as_any().downcast_ref::<StringArray>() else {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Could not downcast file paths array to StringArray",
                ));
            };
            let Some(positions) = columns[1].as_any().downcast_ref::<Int64Array>() else {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Could not downcast positions array to Int64Array",
                ));
            };

            for (file_path, pos) in file_paths.iter().zip(positions.iter()) {
                let (Some(file_path), Some(pos)) = (file_path, pos) else {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        "null values in delete file",
                    ));
                };

                result
                    .entry(file_path.to_string())
                    .or_default()
                    .insert(pos as u64);
            }
        }

        Ok(result)
    }

    async fn parse_equality_deletes_record_batch_stream(
        mut stream: ArrowRecordBatchStream,
        equality_ids: HashSet<i32>,
    ) -> Result<Predicate> {
        let mut row_predicates = Vec::new();
        let mut batch_schema_iceberg: Option<Schema> = None;
        let accessor = EqDelRecordBatchPartnerAccessor;

        while let Some(record_batch) = stream.next().await {
            let record_batch = record_batch?;

            if record_batch.num_columns() == 0 {
                return Ok(AlwaysTrue);
            }

            let schema = match &batch_schema_iceberg {
                Some(schema) => schema,
                None => {
                    let schema = arrow_schema_to_schema(record_batch.schema().as_ref())?;
                    batch_schema_iceberg = Some(schema);
                    batch_schema_iceberg.as_ref().unwrap()
                }
            };

            let root_array: ArrayRef = Arc::new(StructArray::from(record_batch));

            let mut processor = EqDelColumnProcessor::new(&equality_ids);
            visit_schema_with_partner(schema, &root_array, &mut processor, &accessor)?;

            let mut datum_columns_with_names = processor.finish()?;
            if datum_columns_with_names.is_empty() {
                continue;
            }

            // Process the collected columns in lockstep
            #[allow(clippy::len_zero)]
            while datum_columns_with_names[0].0.len() > 0 {
                let mut row_predicate = AlwaysTrue;
                for &mut (ref mut column, ref field_name) in &mut datum_columns_with_names {
                    if let Some(item) = column.next() {
                        let cell_predicate = if let Some(datum) = item? {
                            Reference::new(field_name.clone()).equal_to(datum.clone())
                        } else {
                            Reference::new(field_name.clone()).is_null()
                        };
                        row_predicate = row_predicate.and(cell_predicate)
                    }
                }
                row_predicates.push(row_predicate.not().rewrite_not());
            }
        }

        // All row predicates are combined to a single predicate by creating a balanced binary tree.
        // Using a simple fold would result in a deeply nested predicate that can cause a stack overflow.
        while row_predicates.len() > 1 {
            let mut next_level = Vec::with_capacity(row_predicates.len().div_ceil(2));
            let mut iter = row_predicates.into_iter();
            while let Some(p1) = iter.next() {
                if let Some(p2) = iter.next() {
                    next_level.push(p1.and(p2));
                } else {
                    next_level.push(p1);
                }
            }
            row_predicates = next_level;
        }

        match row_predicates.pop() {
            Some(p) => Ok(p),
            None => Ok(AlwaysTrue),
        }
    }
}

struct EqDelColumnProcessor<'a> {
    equality_ids: &'a HashSet<i32>,
    collected_columns: Vec<(ArrayRef, String, Type)>,
}

impl<'a> EqDelColumnProcessor<'a> {
    fn new(equality_ids: &'a HashSet<i32>) -> Self {
        Self {
            equality_ids,
            collected_columns: Vec::with_capacity(equality_ids.len()),
        }
    }

    #[allow(clippy::type_complexity)]
    fn finish(
        self,
    ) -> Result<
        Vec<(
            Box<dyn ExactSizeIterator<Item = Result<Option<Datum>>>>,
            String,
        )>,
    > {
        self.collected_columns
            .into_iter()
            .map(|(array, field_name, field_type)| {
                let primitive_type = field_type
                    .as_primitive_type()
                    .ok_or_else(|| {
                        Error::new(ErrorKind::Unexpected, "field is not a primitive type")
                    })?
                    .clone();

                let lit_vec = arrow_primitive_to_literal(&array, &field_type)?;
                let datum_iterator: Box<dyn ExactSizeIterator<Item = Result<Option<Datum>>>> =
                    Box::new(lit_vec.into_iter().map(move |c| {
                        c.map(|literal| {
                            literal
                                .as_primitive_literal()
                                .map(|primitive_literal| {
                                    Datum::new(primitive_type.clone(), primitive_literal)
                                })
                                .ok_or(Error::new(
                                    ErrorKind::Unexpected,
                                    "failed to convert to primitive literal",
                                ))
                        })
                        .transpose()
                    }));

                Ok((datum_iterator, field_name))
            })
            .collect::<Result<Vec<_>>>()
    }
}

impl SchemaWithPartnerVisitor<ArrayRef> for EqDelColumnProcessor<'_> {
    type T = ();

    fn schema(&mut self, _schema: &Schema, _partner: &ArrayRef, _value: ()) -> Result<()> {
        Ok(())
    }

    fn field(&mut self, field: &NestedFieldRef, partner: &ArrayRef, _value: ()) -> Result<()> {
        if self.equality_ids.contains(&field.id) && field.field_type.as_primitive_type().is_some() {
            self.collected_columns.push((
                partner.clone(),
                field.name.clone(),
                field.field_type.as_ref().clone(),
            ));
        }
        Ok(())
    }

    fn r#struct(
        &mut self,
        _struct: &StructType,
        _partner: &ArrayRef,
        _results: Vec<()>,
    ) -> Result<()> {
        Ok(())
    }

    fn list(&mut self, _list: &ListType, _partner: &ArrayRef, _value: ()) -> Result<()> {
        Ok(())
    }

    fn map(
        &mut self,
        _map: &MapType,
        _partner: &ArrayRef,
        _key_value: (),
        _value: (),
    ) -> Result<()> {
        Ok(())
    }

    fn primitive(&mut self, _primitive: &PrimitiveType, _partner: &ArrayRef) -> Result<()> {
        Ok(())
    }
}

struct EqDelRecordBatchPartnerAccessor;

impl PartnerAccessor<ArrayRef> for EqDelRecordBatchPartnerAccessor {
    fn struct_partner<'a>(&self, schema_partner: &'a ArrayRef) -> Result<&'a ArrayRef> {
        Ok(schema_partner)
    }

    fn field_partner<'a>(
        &self,
        struct_partner: &'a ArrayRef,
        field: &NestedField,
    ) -> Result<&'a ArrayRef> {
        let Some(struct_array) = struct_partner.as_any().downcast_ref::<StructArray>() else {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "Expected struct array for field extraction",
            ));
        };

        // Find the field by name within the struct
        for (i, field_def) in struct_array.fields().iter().enumerate() {
            if field_def.name() == &field.name {
                return Ok(struct_array.column(i));
            }
        }

        Err(Error::new(
            ErrorKind::Unexpected,
            format!("Field {} not found in parent struct", field.name),
        ))
    }

    fn list_element_partner<'a>(&self, _list_partner: &'a ArrayRef) -> Result<&'a ArrayRef> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "List columns are unsupported in equality deletes",
        ))
    }

    fn map_key_partner<'a>(&self, _map_partner: &'a ArrayRef) -> Result<&'a ArrayRef> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "Map columns are unsupported in equality deletes",
        ))
    }

    fn map_value_partner<'a>(&self, _map_partner: &'a ArrayRef) -> Result<&'a ArrayRef> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "Map columns are unsupported in equality deletes",
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs::File;
    use std::sync::Arc;

    use arrow_array::cast::AsArray;
    use arrow_array::{
        ArrayRef, BinaryArray, Int32Array, Int64Array, RecordBatch, StringArray, StructArray,
    };
    use arrow_schema::{DataType, Field, Fields};
    use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};
    use parquet::basic::Compression;
    use parquet::file::properties::WriterProperties;
    use tempfile::TempDir;

    use super::*;
    use crate::arrow::delete_filter::tests::setup;
    use crate::scan::FileScanTaskDeleteFile;
    use crate::spec::{DataContentType, Schema};

    #[tokio::test]
    async fn test_delete_file_loader_parse_equality_deletes() {
        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().as_os_str().to_str().unwrap();
        let file_io = FileIO::from_path(table_location).unwrap().build().unwrap();

        let eq_delete_file_path = setup_write_equality_delete_file_1(table_location);

        let basic_delete_file_loader = BasicDeleteFileLoader::new(file_io.clone());
        let record_batch_stream = basic_delete_file_loader
            .parquet_to_batch_stream(&eq_delete_file_path)
            .await
            .expect("could not get batch stream");

        let eq_ids = HashSet::from_iter(vec![2, 3, 4, 6, 8]);

        let parsed_eq_delete = CachingDeleteFileLoader::parse_equality_deletes_record_batch_stream(
            record_batch_stream,
            eq_ids,
        )
        .await
        .expect("error parsing batch stream");
        println!("{parsed_eq_delete}");

        let expected = "(((((y != 1) OR (z != 100)) OR (a != \"HELP\")) OR (sa != 4)) OR (b != 62696E6172795F64617461)) AND (((((y != 2) OR (z IS NOT NULL)) OR (a IS NOT NULL)) OR (sa != 5)) OR (b IS NOT NULL))".to_string();

        assert_eq!(parsed_eq_delete.to_string(), expected);
    }

    /// Create a simple field with metadata.
    fn simple_field(name: &str, ty: DataType, nullable: bool, value: &str) -> Field {
        arrow_schema::Field::new(name, ty, nullable).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            value.to_string(),
        )]))
    }

    fn setup_write_equality_delete_file_1(table_location: &str) -> String {
        let col_y_vals = vec![1, 2];
        let col_y = Arc::new(Int64Array::from(col_y_vals)) as ArrayRef;

        let col_z_vals = vec![Some(100), None];
        let col_z = Arc::new(Int64Array::from(col_z_vals)) as ArrayRef;

        let col_a_vals = vec![Some("HELP"), None];
        let col_a = Arc::new(StringArray::from(col_a_vals)) as ArrayRef;

        let col_s = Arc::new(StructArray::from(vec![
            (
                Arc::new(simple_field("sa", DataType::Int32, false, "6")),
                Arc::new(Int32Array::from(vec![4, 5])) as ArrayRef,
            ),
            (
                Arc::new(simple_field("sb", DataType::Utf8, true, "7")),
                Arc::new(StringArray::from(vec![Some("x"), None])) as ArrayRef,
            ),
        ]));

        let col_b_vals = vec![Some(&b"binary_data"[..]), None];
        let col_b = Arc::new(BinaryArray::from(col_b_vals)) as ArrayRef;

        let equality_delete_schema = {
            let struct_field = DataType::Struct(Fields::from(vec![
                simple_field("sa", DataType::Int32, false, "6"),
                simple_field("sb", DataType::Utf8, true, "7"),
            ]));

            let fields = vec![
                Field::new("y", arrow_schema::DataType::Int64, true).with_metadata(HashMap::from(
                    [(PARQUET_FIELD_ID_META_KEY.to_string(), "2".to_string())],
                )),
                Field::new("z", arrow_schema::DataType::Int64, true).with_metadata(HashMap::from(
                    [(PARQUET_FIELD_ID_META_KEY.to_string(), "3".to_string())],
                )),
                Field::new("a", arrow_schema::DataType::Utf8, true).with_metadata(HashMap::from([
                    (PARQUET_FIELD_ID_META_KEY.to_string(), "4".to_string()),
                ])),
                simple_field("s", struct_field, false, "5"),
                simple_field("b", DataType::Binary, true, "8"),
            ];
            Arc::new(arrow_schema::Schema::new(fields))
        };

        let equality_deletes_to_write = RecordBatch::try_new(equality_delete_schema.clone(), vec![
            col_y, col_z, col_a, col_s, col_b,
        ])
        .unwrap();

        let path = format!("{}/equality-deletes-1.parquet", &table_location);

        let file = File::create(&path).unwrap();

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let mut writer = ArrowWriter::try_new(
            file,
            equality_deletes_to_write.schema(),
            Some(props.clone()),
        )
        .unwrap();

        writer
            .write(&equality_deletes_to_write)
            .expect("Writing batch");

        // writer must be closed to write footer
        writer.close().unwrap();

        path
    }

    #[tokio::test]
    async fn test_caching_delete_file_loader_load_deletes() {
        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path();
        let file_io = FileIO::from_path(table_location.as_os_str().to_str().unwrap())
            .unwrap()
            .build()
            .unwrap();

        let delete_file_loader = CachingDeleteFileLoader::new(file_io.clone(), 10);

        let file_scan_tasks = setup(table_location);

        let delete_filter = delete_file_loader
            .load_deletes(&file_scan_tasks[0].deletes, file_scan_tasks[0].schema_ref())
            .await
            .unwrap()
            .unwrap();

        let result = delete_filter
            .get_delete_vector(&file_scan_tasks[0])
            .unwrap();

        // union of pos dels from pos del file 1 and 2, ie
        // [0, 1, 3, 5, 6, 8, 1022, 1023] | [0, 1, 3, 5, 20, 21, 22, 23]
        // = [0, 1, 3, 5, 6, 8, 20, 21, 22, 23, 1022, 1023]
        assert_eq!(result.lock().unwrap().len(), 12);

        let result = delete_filter.get_delete_vector(&file_scan_tasks[1]);
        assert!(result.is_none()); // no pos dels for file 3
    }

    /// Verifies that evolve_schema on partial-schema equality deletes works correctly
    /// when only equality_ids columns are evolved, not all table columns.
    ///
    /// Per the [Iceberg spec](https://iceberg.apache.org/spec/#equality-delete-files),
    /// equality delete files can contain only a subset of columns.
    #[tokio::test]
    async fn test_partial_schema_equality_deletes_evolve_succeeds() {
        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().as_os_str().to_str().unwrap();

        // Create table schema with REQUIRED fields
        let table_schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    crate::spec::NestedField::required(
                        1,
                        "id",
                        crate::spec::Type::Primitive(crate::spec::PrimitiveType::Int),
                    )
                    .into(),
                    crate::spec::NestedField::required(
                        2,
                        "data",
                        crate::spec::Type::Primitive(crate::spec::PrimitiveType::String),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );

        // Write equality delete file with PARTIAL schema (only 'data' column)
        let delete_file_path = {
            let data_vals = vec!["a", "d", "g"];
            let data_col = Arc::new(StringArray::from(data_vals)) as ArrayRef;

            let delete_schema = Arc::new(arrow_schema::Schema::new(vec![simple_field(
                "data",
                DataType::Utf8,
                false,
                "2", // field ID
            )]));

            let delete_batch = RecordBatch::try_new(delete_schema.clone(), vec![data_col]).unwrap();

            let path = format!("{}/partial-eq-deletes.parquet", &table_location);
            let file = File::create(&path).unwrap();
            let props = WriterProperties::builder()
                .set_compression(Compression::SNAPPY)
                .build();
            let mut writer =
                ArrowWriter::try_new(file, delete_batch.schema(), Some(props)).unwrap();
            writer.write(&delete_batch).expect("Writing batch");
            writer.close().unwrap();
            path
        };

        let file_io = FileIO::from_path(table_location).unwrap().build().unwrap();
        let basic_delete_file_loader = BasicDeleteFileLoader::new(file_io.clone());

        let batch_stream = basic_delete_file_loader
            .parquet_to_batch_stream(&delete_file_path)
            .await
            .unwrap();

        // Only evolve the equality_ids columns (field 2), not all table columns
        let equality_ids = vec![2];
        let evolved_stream =
            BasicDeleteFileLoader::evolve_schema(batch_stream, table_schema, &equality_ids)
                .await
                .unwrap();

        let result = evolved_stream.try_collect::<Vec<_>>().await;

        assert!(
            result.is_ok(),
            "Expected success when evolving only equality_ids columns, got error: {:?}",
            result.err()
        );

        let batches = result.unwrap();
        assert_eq!(batches.len(), 1);

        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 1); // Only 'data' column

        // Verify the actual values are preserved after schema evolution
        let data_col = batch.column(0).as_string::<i32>();
        assert_eq!(data_col.value(0), "a");
        assert_eq!(data_col.value(1), "d");
        assert_eq!(data_col.value(2), "g");
    }

    /// Test loading a FileScanTask with BOTH positional and equality deletes.
    /// Verifies the fix for the inverted condition that caused "Missing predicate for equality delete file" errors.
    #[tokio::test]
    async fn test_load_deletes_with_mixed_types() {
        use crate::scan::FileScanTask;
        use crate::spec::{DataFileFormat, Schema};

        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path();
        let file_io = FileIO::from_path(table_location.as_os_str().to_str().unwrap())
            .unwrap()
            .build()
            .unwrap();

        // Create the data file schema
        let data_file_schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    crate::spec::NestedField::optional(
                        2,
                        "y",
                        crate::spec::Type::Primitive(crate::spec::PrimitiveType::Long),
                    )
                    .into(),
                    crate::spec::NestedField::optional(
                        3,
                        "z",
                        crate::spec::Type::Primitive(crate::spec::PrimitiveType::Long),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );

        // Write positional delete file
        let positional_delete_schema = crate::arrow::delete_filter::tests::create_pos_del_schema();
        let file_path_values =
            vec![format!("{}/data-1.parquet", table_location.to_str().unwrap()); 4];
        let file_path_col = Arc::new(StringArray::from_iter_values(&file_path_values));
        let pos_col = Arc::new(Int64Array::from_iter_values(vec![0i64, 1, 2, 3]));

        let positional_deletes_to_write =
            RecordBatch::try_new(positional_delete_schema.clone(), vec![
                file_path_col,
                pos_col,
            ])
            .unwrap();

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let pos_del_path = format!("{}/pos-del-mixed.parquet", table_location.to_str().unwrap());
        let file = File::create(&pos_del_path).unwrap();
        let mut writer = ArrowWriter::try_new(
            file,
            positional_deletes_to_write.schema(),
            Some(props.clone()),
        )
        .unwrap();
        writer.write(&positional_deletes_to_write).unwrap();
        writer.close().unwrap();

        // Write equality delete file
        let eq_delete_path = setup_write_equality_delete_file_1(table_location.to_str().unwrap());

        // Create FileScanTask with BOTH positional and equality deletes
        let pos_del = FileScanTaskDeleteFile {
            file_path: pos_del_path,
            file_type: DataContentType::PositionDeletes,
            partition_spec_id: 0,
            equality_ids: None,
        };

        let eq_del = FileScanTaskDeleteFile {
            file_path: eq_delete_path.clone(),
            file_type: DataContentType::EqualityDeletes,
            partition_spec_id: 0,
            equality_ids: Some(vec![2, 3]), // Only use field IDs that exist in both schemas
        };

        let file_scan_task = FileScanTask {
            start: 0,
            length: 0,
            record_count: None,
            data_file_path: format!("{}/data-1.parquet", table_location.to_str().unwrap()),
            data_file_format: DataFileFormat::Parquet,
            schema: data_file_schema.clone(),
            project_field_ids: vec![2, 3],
            predicate: None,
            deletes: vec![pos_del, eq_del],
            partition: None,
            partition_spec: None,
            name_mapping: None,
            case_sensitive: false,
        };

        // Load the deletes - should handle both types without error
        let delete_file_loader = CachingDeleteFileLoader::new(file_io.clone(), 10);
        let delete_filter = delete_file_loader
            .load_deletes(&file_scan_task.deletes, file_scan_task.schema_ref())
            .await
            .unwrap()
            .unwrap();

        // Verify both delete types can be processed together
        let result = delete_filter
            .build_equality_delete_predicate(&file_scan_task)
            .await;
        assert!(
            result.is_ok(),
            "Failed to build equality delete predicate: {:?}",
            result.err()
        );
    }

    #[tokio::test]
    async fn test_large_equality_delete_batch_stack_overflow() {
        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().as_os_str().to_str().unwrap();
        let file_io = FileIO::from_path(table_location).unwrap().build().unwrap();

        // Create a large batch of equality deletes
        let num_rows = 20_000;
        let col_y_vals: Vec<i64> = (0..num_rows).collect();
        let col_y = Arc::new(Int64Array::from(col_y_vals)) as ArrayRef;

        let schema = Arc::new(arrow_schema::Schema::new(vec![
            Field::new("y", arrow_schema::DataType::Int64, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "2".to_string(),
            )])),
        ]));

        let record_batch = RecordBatch::try_new(schema.clone(), vec![col_y]).unwrap();

        // Write to file
        let path = format!("{}/large-eq-deletes.parquet", &table_location);
        let file = File::create(&path).unwrap();
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let mut writer = ArrowWriter::try_new(file, schema, Some(props)).unwrap();
        writer.write(&record_batch).unwrap();
        writer.close().unwrap();

        let basic_delete_file_loader = BasicDeleteFileLoader::new(file_io.clone());
        let record_batch_stream = basic_delete_file_loader
            .parquet_to_batch_stream(&path)
            .await
            .expect("could not get batch stream");

        let eq_ids = HashSet::from_iter(vec![2]);

        let result = CachingDeleteFileLoader::parse_equality_deletes_record_batch_stream(
            record_batch_stream,
            eq_ids,
        )
        .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_caching_delete_file_loader_caches_results() {
        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path();
        let file_io = FileIO::from_path(table_location.as_os_str().to_str().unwrap())
            .unwrap()
            .build()
            .unwrap();

        let delete_file_loader = CachingDeleteFileLoader::new(file_io.clone(), 10);

        let file_scan_tasks = setup(table_location);

        // Load deletes for the first time
        let delete_filter_1 = delete_file_loader
            .load_deletes(&file_scan_tasks[0].deletes, file_scan_tasks[0].schema_ref())
            .await
            .unwrap()
            .unwrap();

        // Load deletes for the second time (same task/files)
        let delete_filter_2 = delete_file_loader
            .load_deletes(&file_scan_tasks[0].deletes, file_scan_tasks[0].schema_ref())
            .await
            .unwrap()
            .unwrap();

        let dv1 = delete_filter_1
            .get_delete_vector(&file_scan_tasks[0])
            .unwrap();
        let dv2 = delete_filter_2
            .get_delete_vector(&file_scan_tasks[0])
            .unwrap();

        // Verify that the delete vectors point to the same memory location,
        // confirming that the second load reused the result from the first.
        assert!(Arc::ptr_eq(&dv1, &dv2));
    }
}
