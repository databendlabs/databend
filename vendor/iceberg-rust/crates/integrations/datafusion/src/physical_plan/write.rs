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

use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::str::FromStr;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{
    DataType, Field, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef,
};
use datafusion::common::Result as DFResult;
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    execute_input_stream,
};
use futures::StreamExt;
use iceberg::arrow::FieldMatchMode;
use iceberg::spec::{DataFileFormat, TableProperties, serialize_data_file_to_json};
use iceberg::table::Table;
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::{Error, ErrorKind};
use parquet::file::properties::WriterProperties;
use uuid::Uuid;

use crate::physical_plan::DATA_FILES_COL_NAME;
use crate::task_writer::TaskWriter;
use crate::to_datafusion_error;

/// An execution plan node that writes data to an Iceberg table.
///
/// This execution plan takes input data from a child execution plan and writes it to an Iceberg table.
/// It handles the creation of data files in the appropriate format and returns information about the written files as its output.
///
/// The output of this execution plan is a record batch containing a single column with serialized
/// data file information that can be used for committing the write operation to the table.
#[derive(Debug)]
pub(crate) struct IcebergWriteExec {
    table: Table,
    input: Arc<dyn ExecutionPlan>,
    result_schema: ArrowSchemaRef,
    plan_properties: PlanProperties,
}

impl IcebergWriteExec {
    pub fn new(table: Table, input: Arc<dyn ExecutionPlan>, schema: ArrowSchemaRef) -> Self {
        let plan_properties = Self::compute_properties(&input, schema);

        Self {
            table,
            input,
            result_schema: Self::make_result_schema(),
            plan_properties,
        }
    }

    fn compute_properties(
        input: &Arc<dyn ExecutionPlan>,
        schema: ArrowSchemaRef,
    ) -> PlanProperties {
        PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(input.output_partitioning().partition_count()),
            EmissionType::Final,
            Boundedness::Bounded,
        )
    }

    // Create a record batch with serialized data files
    fn make_result_batch(data_files: Vec<String>) -> DFResult<RecordBatch> {
        let files_array = Arc::new(StringArray::from(data_files)) as ArrayRef;

        RecordBatch::try_new(Self::make_result_schema(), vec![files_array]).map_err(|e| {
            DataFusionError::ArrowError(
                Box::new(e),
                Some("Failed to make result batch".to_string()),
            )
        })
    }

    fn make_result_schema() -> ArrowSchemaRef {
        // Define a schema.
        Arc::new(ArrowSchema::new(vec![Field::new(
            DATA_FILES_COL_NAME,
            DataType::Utf8,
            false,
        )]))
    }
}

impl DisplayAs for IcebergWriteExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                write!(f, "IcebergWriteExec: table={}", self.table.identifier())
            }
            DisplayFormatType::Verbose => {
                write!(
                    f,
                    "IcebergWriteExec: table={}, result_schema={:?}",
                    self.table.identifier(),
                    self.result_schema
                )
            }
            DisplayFormatType::TreeRender => {
                write!(f, "IcebergWriteExec: table={}", self.table.identifier())
            }
        }
    }
}

impl ExecutionPlan for IcebergWriteExec {
    fn name(&self) -> &str {
        "IcebergWriteExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Prevents the introduction of additional `RepartitionExec` and processing input in parallel.
    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        // Maintains ordering in the sense that the written file will reflect the ordering of the input.
        vec![true; self.children().len()]
    }

    fn properties(&self) -> &PlanProperties {
        &self.plan_properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(format!(
                "IcebergWriteExec expects exactly one child, but provided {}",
                children.len()
            )));
        }

        Ok(Arc::new(Self::new(
            self.table.clone(),
            Arc::clone(&children[0]),
            self.schema(),
        )))
    }

    /// Executes the write operation for the given partition.
    ///
    /// This function:
    /// 1. Sets up a data file writer based on the table's configuration
    /// 2. Processes input data from the child execution plan
    /// 3. Writes the data to files using the configured writer
    /// 4. Returns a stream containing information about the written data files
    ///
    /// The output of this function is a stream of record batches with the following structure:
    ///
    /// ```text
    /// +------------------+
    /// | data_files       |
    /// +------------------+
    /// | "{"file_path":.. |  <- JSON string representing a data file
    /// +------------------+
    /// ```
    ///
    /// Each row in the output contains a JSON string representing a data file that was written.
    ///
    /// This output can be used by a subsequent operation to commit the added files to the table.
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let partition_type = self.table.metadata().default_partition_type().clone();
        let format_version = self.table.metadata().format_version();

        // Check data file format
        let file_format = DataFileFormat::from_str(
            self.table
                .metadata()
                .properties()
                .get(TableProperties::PROPERTY_DEFAULT_FILE_FORMAT)
                .unwrap_or(&TableProperties::PROPERTY_DEFAULT_FILE_FORMAT_DEFAULT.to_string()),
        )
        .map_err(to_datafusion_error)?;
        if file_format != DataFileFormat::Parquet {
            return Err(to_datafusion_error(Error::new(
                ErrorKind::FeatureUnsupported,
                format!("File format {file_format} is not supported for insert_into yet!"),
            )));
        }

        // Create data file writer builder
        let parquet_file_writer_builder = ParquetWriterBuilder::new_with_match_mode(
            WriterProperties::default(),
            self.table.metadata().current_schema().clone(),
            FieldMatchMode::Name,
        );
        let target_file_size = match self
            .table
            .metadata()
            .properties()
            .get(TableProperties::PROPERTY_WRITE_TARGET_FILE_SIZE_BYTES)
        {
            Some(value_str) => value_str
                .parse::<usize>()
                .map_err(|e| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        "Invalid value for write.target-file-size-bytes",
                    )
                    .with_source(e)
                })
                .map_err(to_datafusion_error)?,
            None => TableProperties::PROPERTY_WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT,
        };

        let file_io = self.table.file_io().clone();
        // todo location_gen and file_name_gen should be configurable
        let location_generator = DefaultLocationGenerator::new(self.table.metadata().clone())
            .map_err(to_datafusion_error)?;
        // todo filename prefix/suffix should be configurable
        let file_name_generator =
            DefaultFileNameGenerator::new(Uuid::now_v7().to_string(), None, file_format);
        let rolling_writer_builder = RollingFileWriterBuilder::new(
            parquet_file_writer_builder,
            target_file_size,
            file_io,
            location_generator,
            file_name_generator,
        );
        let data_file_writer_builder = DataFileWriterBuilder::new(rolling_writer_builder);

        // Create TaskWriter
        let fanout_enabled = self
            .table
            .metadata()
            .properties()
            .get(TableProperties::PROPERTY_DATAFUSION_WRITE_FANOUT_ENABLED)
            .map(|value| {
                value
                    .parse::<bool>()
                    .map_err(|e| {
                        Error::new(
                            ErrorKind::DataInvalid,
                            format!(
                                "Invalid value for {}, expected 'true' or 'false'",
                                TableProperties::PROPERTY_DATAFUSION_WRITE_FANOUT_ENABLED
                            ),
                        )
                        .with_source(e)
                    })
                    .map_err(to_datafusion_error)
            })
            .transpose()?
            .unwrap_or(TableProperties::PROPERTY_DATAFUSION_WRITE_FANOUT_ENABLED_DEFAULT);
        let schema = self.table.metadata().current_schema().clone();
        let partition_spec = self.table.metadata().default_partition_spec().clone();
        let task_writer = TaskWriter::try_new(
            data_file_writer_builder,
            fanout_enabled,
            schema.clone(),
            partition_spec,
        )
        .map_err(to_datafusion_error)?;

        // Get input data
        let data = execute_input_stream(
            Arc::clone(&self.input),
            self.input.schema(), // input schema may have projected column `_partition`
            partition,
            Arc::clone(&context),
        )?;

        // Create write stream
        let stream = futures::stream::once(async move {
            let mut task_writer = task_writer;
            let mut input_stream = data;

            while let Some(batch) = input_stream.next().await {
                let batch = batch?;
                task_writer
                    .write(batch)
                    .await
                    .map_err(to_datafusion_error)?;
            }

            let data_files = task_writer.close().await.map_err(to_datafusion_error)?;

            // Convert builders to data files and then to JSON strings
            let data_files_strs: Vec<String> = data_files
                .into_iter()
                .map(|data_file| {
                    serialize_data_file_to_json(data_file, &partition_type, format_version)
                        .map_err(to_datafusion_error)
                })
                .collect::<DFResult<Vec<String>>>()?;

            Self::make_result_batch(data_files_strs)
        })
        .boxed();

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.result_schema),
            stream,
        )))
    }
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::collections::HashMap;
    use std::fmt::{Debug, Formatter};
    use std::sync::Arc;

    use datafusion::arrow::array::{ArrayRef, Int32Array, RecordBatch, StringArray};
    use datafusion::arrow::datatypes::{
        DataType, Field, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef,
    };
    use datafusion::common::Result as DFResult;
    use datafusion::execution::{SendableRecordBatchStream, TaskContext};
    use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
    use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
    use futures::{StreamExt, stream};
    use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
    use iceberg::spec::{
        DataFileFormat, NestedField, PrimitiveType, Schema, Type, deserialize_data_file_from_json,
    };
    use iceberg::{Catalog, CatalogBuilder, MemoryCatalog, NamespaceIdent, Result, TableCreation};
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
    use tempfile::TempDir;

    use super::*;

    /// A simple execution plan that returns a predefined set of record batches
    struct MockExecutionPlan {
        schema: ArrowSchemaRef,
        batches: Vec<RecordBatch>,
        properties: PlanProperties,
    }

    impl MockExecutionPlan {
        fn new(schema: ArrowSchemaRef, batches: Vec<RecordBatch>) -> Self {
            let properties = PlanProperties::new(
                EquivalenceProperties::new(schema.clone()),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Final,
                Boundedness::Bounded,
            );

            Self {
                schema,
                batches,
                properties,
            }
        }
    }

    impl Debug for MockExecutionPlan {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "MockExecutionPlan")
        }
    }

    impl DisplayAs for MockExecutionPlan {
        fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
            match t {
                DisplayFormatType::Default
                | DisplayFormatType::Verbose
                | DisplayFormatType::TreeRender => {
                    write!(f, "MockExecutionPlan")
                }
            }
        }
    }

    impl ExecutionPlan for MockExecutionPlan {
        fn name(&self) -> &str {
            "MockExecutionPlan"
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn properties(&self) -> &PlanProperties {
            &self.properties
        }

        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![]
        }

        fn with_new_children(
            self: Arc<Self>,
            _children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> DFResult<Arc<dyn ExecutionPlan>> {
            Ok(self)
        }

        fn execute(
            &self,
            _partition: usize,
            _context: Arc<TaskContext>,
        ) -> DFResult<SendableRecordBatchStream> {
            let batches = self.batches.clone();
            let stream = stream::iter(batches.into_iter().map(Ok));
            Ok(Box::pin(RecordBatchStreamAdapter::new(
                self.schema.clone(),
                stream.boxed(),
            )))
        }
    }

    /// Helper function to create a temporary directory and return its path
    fn temp_path() -> String {
        let temp_dir = TempDir::new().unwrap();
        temp_dir.path().to_str().unwrap().to_string()
    }

    /// Helper function to create a memory catalog
    async fn get_iceberg_catalog() -> MemoryCatalog {
        MemoryCatalogBuilder::default()
            .load(
                "memory",
                HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), temp_path())]),
            )
            .await
            .unwrap()
    }

    /// Helper function to create a test table schema
    fn get_test_schema() -> Result<Schema> {
        Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
    }

    /// Helper function to create a table creation
    fn get_table_creation(
        location: impl ToString,
        name: impl ToString,
        schema: Schema,
    ) -> TableCreation {
        TableCreation::builder()
            .location(location.to_string())
            .name(name.to_string())
            .properties(HashMap::new())
            .schema(schema)
            .build()
    }

    #[tokio::test]
    async fn test_iceberg_write_exec() -> Result<()> {
        // 1. Set up test environment
        let iceberg_catalog = get_iceberg_catalog().await;
        let namespace = NamespaceIdent::new("test_namespace".to_string());

        // Create namespace
        iceberg_catalog
            .create_namespace(&namespace, HashMap::new())
            .await?;

        // Create schema
        let schema = get_test_schema()?;

        // Create table
        let table_name = "test_table";
        let table_location = temp_path();
        let creation = get_table_creation(table_location, table_name, schema);
        let table = iceberg_catalog.create_table(&namespace, creation).await?;

        // 2. Create test data
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "1".to_string(),
            )])),
            Field::new("name", DataType::Utf8, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "2".to_string(),
            )])),
        ]));

        let id_array = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
        let name_array = Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])) as ArrayRef;

        let batch = RecordBatch::try_new(arrow_schema.clone(), vec![id_array, name_array])
            .map_err(|e| {
                Error::new(
                    ErrorKind::Unexpected,
                    format!("Failed to create record batch: {e}"),
                )
            })?;

        // 3. Create mock input execution plan
        let input_plan = Arc::new(MockExecutionPlan::new(arrow_schema.clone(), vec![
            batch.clone(),
        ]));

        // 4. Create IcebergWriteExec
        let write_exec = IcebergWriteExec::new(table.clone(), input_plan, arrow_schema);

        // 5. Execute the plan
        let task_ctx = Arc::new(TaskContext::default());
        let stream = write_exec.execute(0, task_ctx).map_err(|e| {
            Error::new(
                ErrorKind::Unexpected,
                format!("Failed to execute plan: {e}"),
            )
        })?;

        // Collect the results
        let mut results = vec![];
        let mut stream = stream;
        while let Some(batch) = stream.next().await {
            results.push(batch.map_err(|e| {
                Error::new(ErrorKind::Unexpected, format!("Failed to get batch: {e}"))
            })?);
        }

        // 6. Verify the results
        assert_eq!(results.len(), 1, "Expected one result batch");
        let result_batch = &results[0];

        // Check schema
        assert_eq!(
            result_batch.schema().as_ref(),
            &ArrowSchema::new(vec![Field::new(DATA_FILES_COL_NAME, DataType::Utf8, false)])
        );

        // Check data
        assert_eq!(result_batch.num_rows(), 1, "Expected one data file");

        // Get the data file JSON
        let data_file_json = result_batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Expected StringArray")
            .value(0);

        // Deserialize the data file JSON
        let partition_type = table.metadata().default_partition_type();
        let spec_id = table.metadata().default_partition_spec_id();
        let schema = table.metadata().current_schema();

        let data_file =
            deserialize_data_file_from_json(data_file_json, spec_id, partition_type, schema)
                .expect("Failed to deserialize data file JSON");

        // Verify data file properties
        assert_eq!(
            data_file.record_count(),
            3,
            "Expected 3 records in the data file"
        );
        assert!(
            data_file.file_size_in_bytes() > 0,
            "File size should be greater than 0"
        );
        assert_eq!(
            data_file.file_format(),
            DataFileFormat::Parquet,
            "Expected Parquet file format"
        );

        // Verify column statistics
        assert!(
            data_file.column_sizes().get(&1).unwrap() > &0,
            "Column 1 size should be greater than 0"
        );
        assert!(
            data_file.column_sizes().get(&2).unwrap() > &0,
            "Column 2 size should be greater than 0"
        );

        assert_eq!(
            *data_file.value_counts().get(&1).unwrap(),
            3,
            "Expected 3 values for column 1"
        );
        assert_eq!(
            *data_file.value_counts().get(&2).unwrap(),
            3,
            "Expected 3 values for column 2"
        );

        // Verify lower and upper bounds
        assert!(
            data_file.lower_bounds().contains_key(&1) || data_file.lower_bounds().contains_key(&2),
            "Expected lower bounds to contain at least one column"
        );
        assert!(
            data_file.upper_bounds().contains_key(&1) || data_file.upper_bounds().contains_key(&2),
            "Expected upper bounds to contain at least one column"
        );

        // Check that the file path exists
        let file_path = data_file.file_path();
        assert!(!file_path.is_empty(), "File path should not be empty");

        // 7. Verify the file exists
        let file_io = table.file_io();
        assert!(file_io.exists(file_path).await?, "Data file should exist");

        Ok(())
    }
}
