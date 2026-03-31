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
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, RecordBatch, StringArray, UInt64Array};
use datafusion::arrow::datatypes::{
    DataType, Field, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef,
};
use datafusion::common::{DataFusionError, Result as DFResult};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::StreamExt;
use iceberg::Catalog;
use iceberg::spec::{DataFile, deserialize_data_file_from_json};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};

use crate::physical_plan::DATA_FILES_COL_NAME;
use crate::to_datafusion_error;

/// IcebergCommitExec is responsible for collecting the files written and use
/// [`Transaction::fast_append`] to commit the data files written.
#[derive(Debug)]
pub(crate) struct IcebergCommitExec {
    table: Table,
    catalog: Arc<dyn Catalog>,
    input: Arc<dyn ExecutionPlan>,
    schema: ArrowSchemaRef,
    count_schema: ArrowSchemaRef,
    plan_properties: PlanProperties,
}

impl IcebergCommitExec {
    pub fn new(
        table: Table,
        catalog: Arc<dyn Catalog>,
        input: Arc<dyn ExecutionPlan>,
        schema: ArrowSchemaRef,
    ) -> Self {
        let count_schema = Self::make_count_schema();

        let plan_properties = Self::compute_properties(Arc::clone(&count_schema));

        Self {
            table,
            catalog,
            input,
            schema,
            count_schema,
            plan_properties,
        }
    }

    // Compute the plan properties for this execution plan
    fn compute_properties(schema: ArrowSchemaRef) -> PlanProperties {
        PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        )
    }

    // Create a record batch with just the count of rows written
    fn make_count_batch(count: u64) -> DFResult<RecordBatch> {
        let count_array = Arc::new(UInt64Array::from(vec![count])) as ArrayRef;

        RecordBatch::try_from_iter_with_nullable(vec![("count", count_array, false)]).map_err(|e| {
            DataFusionError::ArrowError(
                Box::new(e),
                Some("Failed to make count batch!".to_string()),
            )
        })
    }

    fn make_count_schema() -> ArrowSchemaRef {
        // Define a schema.
        Arc::new(ArrowSchema::new(vec![Field::new(
            "count",
            DataType::UInt64,
            false,
        )]))
    }
}

impl DisplayAs for IcebergCommitExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                write!(f, "IcebergCommitExec: table={}", self.table.identifier())
            }
            DisplayFormatType::Verbose => {
                write!(
                    f,
                    "IcebergCommitExec: table={}, schema={:?}",
                    self.table.identifier(),
                    self.schema
                )
            }
            DisplayFormatType::TreeRender => {
                write!(f, "IcebergCommitExec: table={}", self.table.identifier())
            }
        }
    }
}

impl ExecutionPlan for IcebergCommitExec {
    fn name(&self) -> &str {
        "IcebergCommitExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.plan_properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn required_input_distribution(&self) -> Vec<datafusion::physical_plan::Distribution> {
        vec![datafusion::physical_plan::Distribution::SinglePartition; self.children().len()]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(format!(
                "IcebergCommitExec expects exactly one child, but provided {}",
                children.len()
            )));
        }

        Ok(Arc::new(IcebergCommitExec::new(
            self.table.clone(),
            self.catalog.clone(),
            children[0].clone(),
            self.schema.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        // IcebergCommitExec only has one partition (partition 0)
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "IcebergCommitExec only has one partition, but got partition {partition}"
            )));
        }

        let table = self.table.clone();
        let input_plan = self.input.clone();
        let count_schema = Arc::clone(&self.count_schema);

        // todo revisit this
        let spec_id = self.table.metadata().default_partition_spec_id();
        let partition_type = self.table.metadata().default_partition_type().clone();
        let current_schema = self.table.metadata().current_schema().clone();

        let catalog = Arc::clone(&self.catalog);

        // Process the input streams from all partitions and commit the data files
        let stream = futures::stream::once(async move {
            let mut data_files: Vec<DataFile> = Vec::new();
            let mut total_record_count: u64 = 0;

            // Execute and collect results from the input coalesced plan
            let mut batch_stream = input_plan.execute(0, context)?;

            while let Some(batch_result) = batch_stream.next().await {
                let batch = batch_result?;

                let files_array = batch
                    .column_by_name(DATA_FILES_COL_NAME)
                    .ok_or_else(|| {
                        DataFusionError::Internal(
                            "Expected 'data_files' column in input batch".to_string(),
                        )
                    })?
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        DataFusionError::Internal(
                            "Expected 'data_files' column to be StringArray".to_string(),
                        )
                    })?;

                // Deserialize all data files from the StringArray
                let batch_files: Vec<DataFile> = files_array
                    .into_iter()
                    .flatten()
                    .map(|f| -> DFResult<DataFile> {
                        // Parse JSON to DataFileSerde and convert to DataFile
                        deserialize_data_file_from_json(
                            f,
                            spec_id,
                            &partition_type,
                            &current_schema,
                        )
                        .map_err(to_datafusion_error)
                    })
                    .collect::<datafusion::common::Result<_>>()?;

                // add record_counts from the current batch to total record count
                total_record_count += batch_files.iter().map(|f| f.record_count()).sum::<u64>();

                // Add all deserialized files to our collection
                data_files.extend(batch_files);
            }

            // If no data files were collected, return an empty result
            if data_files.is_empty() {
                return Ok(RecordBatch::new_empty(count_schema));
            }

            // Create a transaction and commit the data files
            let tx = Transaction::new(&table);
            let action = tx.fast_append().add_data_files(data_files);

            // Apply the action and commit the transaction
            let _updated_table = action
                .apply(tx)
                .map_err(to_datafusion_error)?
                .commit(catalog.as_ref())
                .await
                .map_err(to_datafusion_error)?;

            Self::make_count_batch(total_record_count)
        })
        .boxed();

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.count_schema),
            stream,
        )))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fmt;
    use std::sync::Arc;

    use datafusion::arrow::array::{ArrayRef, Int32Array, RecordBatch, StringArray, UInt64Array};
    use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use datafusion::datasource::MemTable;
    use datafusion::execution::context::TaskContext;
    use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
    use datafusion::physical_plan::common::collect;
    use datafusion::physical_plan::execution_plan::Boundedness;
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
    use datafusion::prelude::*;
    use futures::StreamExt;
    use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
    use iceberg::spec::{
        DataContentType, DataFileBuilder, DataFileFormat, NestedField, PrimitiveType, Schema,
        Struct, Type,
    };
    use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};

    use super::*;
    use crate::physical_plan::DATA_FILES_COL_NAME;
    use crate::table::IcebergTableProvider;

    // A mock execution plan that returns record batches with serialized data files
    #[derive(Debug)]
    struct MockWriteExec {
        schema: Arc<ArrowSchema>,
        data_files_json: Vec<String>,
        plan_properties: PlanProperties,
    }

    impl MockWriteExec {
        fn new(data_files_json: Vec<String>) -> Self {
            let schema = Arc::new(ArrowSchema::new(vec![Field::new(
                DATA_FILES_COL_NAME,
                DataType::Utf8,
                false,
            )]));

            let plan_properties = PlanProperties::new(
                EquivalenceProperties::new(schema.clone()),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Final,
                Boundedness::Bounded,
            );

            Self {
                schema,
                data_files_json,
                plan_properties,
            }
        }
    }

    impl ExecutionPlan for MockWriteExec {
        fn name(&self) -> &str {
            "MockWriteExec"
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn schema(&self) -> Arc<ArrowSchema> {
            self.schema.clone()
        }

        fn properties(&self) -> &PlanProperties {
            &self.plan_properties
        }

        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![]
        }

        fn with_new_children(
            self: Arc<Self>,
            _children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
            Ok(self)
        }

        fn execute(
            &self,
            _partition: usize,
            _context: Arc<TaskContext>,
        ) -> datafusion::common::Result<SendableRecordBatchStream> {
            // Create a record batch with the serialized data files
            let array = Arc::new(StringArray::from(self.data_files_json.clone())) as ArrayRef;
            let batch = RecordBatch::try_new(self.schema.clone(), vec![array])?;

            // Create a stream that returns this batch
            let stream = futures::stream::once(async move { Ok(batch) }).boxed();
            Ok(Box::pin(RecordBatchStreamAdapter::new(
                self.schema(),
                stream,
            )))
        }
    }

    // Implement DisplayAs for MockDataFilesExec
    impl DisplayAs for MockWriteExec {
        fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
            match t {
                DisplayFormatType::Default
                | DisplayFormatType::Verbose
                | DisplayFormatType::TreeRender => {
                    write!(f, "MockDataFilesExec: files={}", self.data_files_json.len())
                }
            }
        }
    }

    #[tokio::test]
    async fn test_iceberg_commit_exec() -> Result<(), Box<dyn std::error::Error>> {
        // Create a memory catalog with in-memory file IO
        let catalog = Arc::new(
            MemoryCatalogBuilder::default()
                .load(
                    "memory",
                    HashMap::from([(
                        MEMORY_CATALOG_WAREHOUSE.to_string(),
                        "memory://root".to_string(),
                    )]),
                )
                .await
                .unwrap(),
        );

        // Create a namespace
        let namespace = NamespaceIdent::new("test_namespace".to_string());
        catalog.create_namespace(&namespace, HashMap::new()).await?;

        // Create a schema for the table
        let schema = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()?;

        // Create a table
        let table_creation = TableCreation::builder()
            .name("test_table".to_string())
            .schema(schema)
            .location("memory://root/test_table".to_string())
            .properties(HashMap::new())
            .build();

        let table = catalog.create_table(&namespace, table_creation).await?;

        // Create data files
        let data_file1 = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("path/to/file1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(1024)
            .record_count(100)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::empty())
            .build()?;

        let data_file2 = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("path/to/file2.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(2048)
            .record_count(200)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::empty())
            .build()?;

        // Serialize data files to JSON
        let partition_type = table.metadata().default_partition_type().clone();
        let data_file1_json = iceberg::spec::serialize_data_file_to_json(
            data_file1.clone(),
            &partition_type,
            table.metadata().format_version(),
        )?;

        let data_file2_json = iceberg::spec::serialize_data_file_to_json(
            data_file2.clone(),
            &partition_type,
            table.metadata().format_version(),
        )?;

        // Create a mock execution plan that returns the serialized data files
        let input_exec = Arc::new(MockWriteExec::new(vec![data_file1_json, data_file2_json]));

        // Create the IcebergCommitExec
        let arrow_schema = Arc::new(ArrowSchema::new(vec![Field::new(
            DATA_FILES_COL_NAME,
            DataType::Utf8,
            false,
        )]));

        let commit_exec =
            IcebergCommitExec::new(table.clone(), catalog.clone(), input_exec, arrow_schema);

        // Verify Execution Plan schema matches the count schema
        assert_eq!(commit_exec.schema(), IcebergCommitExec::make_count_schema());

        // Execute the commit exec
        let task_ctx = Arc::new(TaskContext::default());
        let stream = commit_exec.execute(0, task_ctx)?;
        let batches = collect(stream).await?;

        // Verify the results
        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.num_columns(), 1);
        assert_eq!(batch.num_rows(), 1);

        // The output should be a record batch with a single column "count" and a single row
        // with the total record count (100 + 200 = 300)
        let count_array = batch.column(0);
        assert_eq!(count_array.len(), 1);
        assert_eq!(count_array.data_type(), &DataType::UInt64);

        // Verify that the count is correct
        let count = count_array.as_any().downcast_ref::<UInt64Array>().unwrap();
        assert_eq!(count.value(0), 300);

        // Verify that the table has been updated with the new files
        let updated_table = catalog
            .load_table(&TableIdent::from_strs(["test_namespace", "test_table"]).unwrap())
            .await?;
        let current_snapshot = updated_table.metadata().current_snapshot().unwrap();

        // Load the manifest list to verify the data files were added
        let manifest_list = current_snapshot
            .load_manifest_list(updated_table.file_io(), updated_table.metadata())
            .await?;

        // There should be at least one manifest
        assert!(!manifest_list.entries().is_empty());

        // Load the first manifest and verify it contains our data files
        let manifest = manifest_list.entries()[0]
            .load_manifest(updated_table.file_io())
            .await?;

        // Verify that the manifest contains our data files
        let manifest_files: Vec<String> = manifest
            .entries()
            .iter()
            .map(|entry| entry.data_file().file_path().to_string())
            .collect();

        assert!(manifest_files.contains(&"path/to/file1.parquet".to_string()));
        assert!(manifest_files.contains(&"path/to/file2.parquet".to_string()));

        Ok(())
    }

    #[tokio::test]
    async fn test_datafusion_execution_partitioned_source() -> Result<(), Box<dyn std::error::Error>>
    {
        let catalog = Arc::new(
            MemoryCatalogBuilder::default()
                .load(
                    "memory",
                    HashMap::from([(
                        MEMORY_CATALOG_WAREHOUSE.to_string(),
                        "memory://root".to_string(),
                    )]),
                )
                .await?,
        );

        let namespace = NamespaceIdent::new("test_namespace".to_string());
        catalog.create_namespace(&namespace, HashMap::new()).await?;

        let schema = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()?;

        let table_name = "test_table";
        let table_creation = TableCreation::builder()
            .name(table_name.to_string())
            .schema(schema)
            .location("memory://root/test_table".to_string())
            .properties(HashMap::new())
            .build();
        let _ = catalog.create_table(&namespace, table_creation).await?;

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let batches: Vec<RecordBatch> = (1..4)
            .map(|idx| {
                RecordBatch::try_new(arrow_schema.clone(), vec![
                    Arc::new(Int32Array::from(vec![idx])) as ArrayRef,
                    Arc::new(StringArray::from(vec![format!("Name{idx}")])) as ArrayRef,
                ])
            })
            .collect::<Result<_, _>>()?;

        // Create DataFusion context with specific partition configuration
        let mut config = SessionConfig::new();
        config = config.set_usize("datafusion.execution.target_partitions", 8);
        let ctx = SessionContext::new_with_config(config);

        // Create multiple partitions - each batch becomes a separate partition
        let partitions: Vec<Vec<RecordBatch>> =
            batches.into_iter().map(|batch| vec![batch]).collect();
        let source_table = Arc::new(MemTable::try_new(Arc::clone(&arrow_schema), partitions)?);
        ctx.register_table("source_table", source_table)?;

        let iceberg_table_provider = IcebergTableProvider::try_new(
            catalog.clone(),
            namespace.clone(),
            table_name.to_string(),
        )
        .await?;
        ctx.register_table("iceberg_table", Arc::new(iceberg_table_provider))?;

        let insert_plan = ctx
            .sql("INSERT INTO iceberg_table SELECT * FROM source_table")
            .await?;

        let physical_plan = insert_plan.create_physical_plan().await?;

        let actual_plan = format!(
            "{}",
            datafusion::physical_plan::displayable(physical_plan.as_ref()).indent(false)
        );

        println!("Physical plan:\n{actual_plan}");

        let expected_plan = "\
IcebergCommitExec: table=test_namespace.test_table
  CoalescePartitionsExec
    IcebergWriteExec: table=test_namespace.test_table
      DataSourceExec: partitions=3, partition_sizes=[1, 1, 1]";

        assert_eq!(
            actual_plan.trim(),
            expected_plan.trim(),
            "Physical plan does not match expected\n\nExpected:\n{}\n\nActual:\n{}",
            expected_plan.trim(),
            actual_plan.trim()
        );

        Ok(())
    }
}
