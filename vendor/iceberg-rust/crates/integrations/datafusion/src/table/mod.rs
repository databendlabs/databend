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

//! Iceberg table providers for DataFusion.
//!
//! This module provides two table provider implementations:
//!
//! - [`IcebergTableProvider`]: Catalog-backed provider with automatic metadata refresh.
//!   Use for write operations and when you need to see the latest table state.
//!
//! - [`IcebergStaticTableProvider`]: Static provider for read-only access to a specific
//!   table snapshot. Use for consistent analytical queries or time-travel scenarios.

pub mod metadata_table;
pub mod table_provider_factory;

use std::any::Any;
use std::num::NonZeroUsize;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::catalog::Session;
use datafusion::common::DataFusionError;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::inspect::MetadataTableType;
use iceberg::table::Table;
use iceberg::{Catalog, Error, ErrorKind, NamespaceIdent, Result, TableIdent};
use metadata_table::IcebergMetadataTableProvider;

use crate::error::to_datafusion_error;
use crate::physical_plan::commit::IcebergCommitExec;
use crate::physical_plan::project::project_with_partition;
use crate::physical_plan::repartition::repartition;
use crate::physical_plan::scan::IcebergTableScan;
use crate::physical_plan::write::IcebergWriteExec;

/// Catalog-backed table provider with automatic metadata refresh.
///
/// This provider loads fresh table metadata from the catalog on every scan and write
/// operation, ensuring you always see the latest table state. Use this when you need
/// write operations or want to see the most up-to-date data.
///
/// For read-only access to a specific snapshot without catalog overhead, use
/// [`IcebergStaticTableProvider`] instead.
#[derive(Debug, Clone)]
pub struct IcebergTableProvider {
    /// The catalog that manages this table
    catalog: Arc<dyn Catalog>,
    /// The table identifier (namespace + name)
    table_ident: TableIdent,
    /// A reference-counted arrow `Schema` (cached at construction)
    schema: ArrowSchemaRef,
}

impl IcebergTableProvider {
    /// Creates a new catalog-backed table provider.
    ///
    /// Loads the table once to get the initial schema, then stores the catalog
    /// reference for future metadata refreshes on each operation.
    pub(crate) async fn try_new(
        catalog: Arc<dyn Catalog>,
        namespace: NamespaceIdent,
        name: impl Into<String>,
    ) -> Result<Self> {
        let table_ident = TableIdent::new(namespace, name.into());

        // Load table once to get initial schema
        let table = catalog.load_table(&table_ident).await?;
        let schema = Arc::new(schema_to_arrow_schema(table.metadata().current_schema())?);

        Ok(IcebergTableProvider {
            catalog,
            table_ident,
            schema,
        })
    }

    pub(crate) async fn metadata_table(
        &self,
        r#type: MetadataTableType,
    ) -> Result<IcebergMetadataTableProvider> {
        // Load fresh table metadata for metadata table access
        let table = self.catalog.load_table(&self.table_ident).await?;
        Ok(IcebergMetadataTableProvider { table, r#type })
    }
}

#[async_trait]
impl TableProvider for IcebergTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        // Load fresh table metadata from catalog
        let table = self
            .catalog
            .load_table(&self.table_ident)
            .await
            .map_err(to_datafusion_error)?;

        // Create scan with fresh metadata (always use current snapshot)
        Ok(Arc::new(IcebergTableScan::new(
            table,
            None, // Always use current snapshot for catalog-backed provider
            self.schema.clone(),
            projection,
            filters,
        )))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        // Push down all filters, as a single source of truth, the scanner will drop the filters which couldn't be push down
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        _insert_op: InsertOp,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        // Load fresh table metadata from catalog
        let table = self
            .catalog
            .load_table(&self.table_ident)
            .await
            .map_err(to_datafusion_error)?;

        let partition_spec = table.metadata().default_partition_spec();

        // Step 1: Project partition values for partitioned tables
        let plan_with_partition = if !partition_spec.is_unpartitioned() {
            project_with_partition(input, &table)?
        } else {
            input
        };

        // Step 2: Repartition for parallel processing
        let target_partitions =
            NonZeroUsize::new(state.config().target_partitions()).ok_or_else(|| {
                DataFusionError::Configuration(
                    "target_partitions must be greater than 0".to_string(),
                )
            })?;

        let repartitioned_plan =
            repartition(plan_with_partition, table.metadata_ref(), target_partitions)?;

        let write_plan = Arc::new(IcebergWriteExec::new(
            table.clone(),
            repartitioned_plan,
            self.schema.clone(),
        ));

        // Merge the outputs of write_plan into one so we can commit all files together
        let coalesce_partitions = Arc::new(CoalescePartitionsExec::new(write_plan));

        Ok(Arc::new(IcebergCommitExec::new(
            table,
            self.catalog.clone(),
            coalesce_partitions,
            self.schema.clone(),
        )))
    }
}

/// Static table provider for read-only snapshot access.
///
/// This provider holds a cached table instance and does not refresh metadata or support
/// write operations. Use this for consistent analytical queries, time-travel scenarios,
/// or when you want to avoid catalog overhead.
///
/// For catalog-backed tables with write support and automatic refresh, use
/// [`IcebergTableProvider`] instead.
#[derive(Debug, Clone)]
pub struct IcebergStaticTableProvider {
    /// The static table instance (never refreshed)
    table: Table,
    /// Optional snapshot ID for this static view
    snapshot_id: Option<i64>,
    /// A reference-counted arrow `Schema`
    schema: ArrowSchemaRef,
}

impl IcebergStaticTableProvider {
    /// Creates a static provider from a table instance.
    ///
    /// Uses the table's current snapshot for all queries. Does not support write operations.
    pub async fn try_new_from_table(table: Table) -> Result<Self> {
        let schema = Arc::new(schema_to_arrow_schema(table.metadata().current_schema())?);
        Ok(IcebergStaticTableProvider {
            table,
            snapshot_id: None,
            schema,
        })
    }

    /// Creates a static provider for a specific table snapshot.
    ///
    /// Queries the specified snapshot for all operations. Useful for time-travel queries.
    /// Does not support write operations.
    pub async fn try_new_from_table_snapshot(table: Table, snapshot_id: i64) -> Result<Self> {
        let snapshot = table
            .metadata()
            .snapshot_by_id(snapshot_id)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::Unexpected,
                    format!(
                        "snapshot id {snapshot_id} not found in table {}",
                        table.identifier().name()
                    ),
                )
            })?;
        let table_schema = snapshot.schema(table.metadata())?;
        let schema = Arc::new(schema_to_arrow_schema(&table_schema)?);
        Ok(IcebergStaticTableProvider {
            table,
            snapshot_id: Some(snapshot_id),
            schema,
        })
    }
}

#[async_trait]
impl TableProvider for IcebergStaticTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        // Use cached table (no refresh)
        Ok(Arc::new(IcebergTableScan::new(
            self.table.clone(),
            self.snapshot_id,
            self.schema.clone(),
            projection,
            filters,
        )))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        // Push down all filters, as a single source of truth, the scanner will drop the filters which couldn't be push down
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    async fn insert_into(
        &self,
        _state: &dyn Session,
        _input: Arc<dyn ExecutionPlan>,
        _insert_op: InsertOp,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Err(to_datafusion_error(Error::new(
            ErrorKind::FeatureUnsupported,
            "Write operations are not supported on IcebergStaticTableProvider. \
             Use IcebergTableProvider with a catalog for write support."
                .to_string(),
        )))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use datafusion::common::Column;
    use datafusion::prelude::SessionContext;
    use iceberg::io::FileIO;
    use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
    use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
    use iceberg::table::{StaticTable, Table};
    use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
    use tempfile::TempDir;

    use super::*;

    async fn get_test_table_from_metadata_file() -> Table {
        let metadata_file_name = "TableMetadataV2Valid.json";
        let metadata_file_path = format!(
            "{}/tests/test_data/{}",
            env!("CARGO_MANIFEST_DIR"),
            metadata_file_name
        );
        let file_io = FileIO::from_path(&metadata_file_path)
            .unwrap()
            .build()
            .unwrap();
        let static_identifier = TableIdent::from_strs(["static_ns", "static_table"]).unwrap();
        let static_table =
            StaticTable::from_metadata_file(&metadata_file_path, static_identifier, file_io)
                .await
                .unwrap();
        static_table.into_table()
    }

    async fn get_test_catalog_and_table() -> (Arc<dyn Catalog>, NamespaceIdent, String, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let warehouse_path = temp_dir.path().to_str().unwrap().to_string();

        let catalog = MemoryCatalogBuilder::default()
            .load(
                "memory",
                HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse_path.clone())]),
            )
            .await
            .unwrap();

        let namespace = NamespaceIdent::new("test_ns".to_string());
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .unwrap();

        let schema = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .unwrap();

        let table_creation = TableCreation::builder()
            .name("test_table".to_string())
            .location(format!("{warehouse_path}/test_table"))
            .schema(schema)
            .properties(HashMap::new())
            .build();

        catalog
            .create_table(&namespace, table_creation)
            .await
            .unwrap();

        (
            Arc::new(catalog),
            namespace,
            "test_table".to_string(),
            temp_dir,
        )
    }

    // Tests for IcebergStaticTableProvider

    #[tokio::test]
    async fn test_static_provider_from_table() {
        let table = get_test_table_from_metadata_file().await;
        let table_provider = IcebergStaticTableProvider::try_new_from_table(table.clone())
            .await
            .unwrap();
        let ctx = SessionContext::new();
        ctx.register_table("mytable", Arc::new(table_provider))
            .unwrap();
        let df = ctx.sql("SELECT * FROM mytable").await.unwrap();
        let df_schema = df.schema();
        let df_columns = df_schema.fields();
        assert_eq!(df_columns.len(), 3);
        let x_column = df_columns.first().unwrap();
        let column_data = format!(
            "{:?}:{:?}",
            x_column.name(),
            x_column.data_type().to_string()
        );
        assert_eq!(column_data, "\"x\":\"Int64\"");
        let has_column = df_schema.has_column(&Column::from_name("z"));
        assert!(has_column);
    }

    #[tokio::test]
    async fn test_static_provider_from_snapshot() {
        let table = get_test_table_from_metadata_file().await;
        let snapshot_id = table.metadata().snapshots().next().unwrap().snapshot_id();
        let table_provider =
            IcebergStaticTableProvider::try_new_from_table_snapshot(table.clone(), snapshot_id)
                .await
                .unwrap();
        let ctx = SessionContext::new();
        ctx.register_table("mytable", Arc::new(table_provider))
            .unwrap();
        let df = ctx.sql("SELECT * FROM mytable").await.unwrap();
        let df_schema = df.schema();
        let df_columns = df_schema.fields();
        assert_eq!(df_columns.len(), 3);
        let x_column = df_columns.first().unwrap();
        let column_data = format!(
            "{:?}:{:?}",
            x_column.name(),
            x_column.data_type().to_string()
        );
        assert_eq!(column_data, "\"x\":\"Int64\"");
        let has_column = df_schema.has_column(&Column::from_name("z"));
        assert!(has_column);
    }

    #[tokio::test]
    async fn test_static_provider_rejects_writes() {
        let table = get_test_table_from_metadata_file().await;
        let table_provider = IcebergStaticTableProvider::try_new_from_table(table.clone())
            .await
            .unwrap();
        let ctx = SessionContext::new();
        ctx.register_table("mytable", Arc::new(table_provider))
            .unwrap();

        // Attempt to insert into the static provider should fail
        let result = ctx.sql("INSERT INTO mytable VALUES (1, 2, 3)").await;

        // The error should occur during planning or execution
        // We expect an error indicating write operations are not supported
        assert!(
            result.is_err() || {
                let df = result.unwrap();
                df.collect().await.is_err()
            }
        );
    }

    #[tokio::test]
    async fn test_static_provider_scan() {
        let table = get_test_table_from_metadata_file().await;
        let table_provider = IcebergStaticTableProvider::try_new_from_table(table.clone())
            .await
            .unwrap();
        let ctx = SessionContext::new();
        ctx.register_table("mytable", Arc::new(table_provider))
            .unwrap();

        // Test that scan operations work correctly
        let df = ctx.sql("SELECT count(*) FROM mytable").await.unwrap();
        let physical_plan = df.create_physical_plan().await;
        assert!(physical_plan.is_ok());
    }

    // Tests for IcebergTableProvider

    #[tokio::test]
    async fn test_catalog_backed_provider_creation() {
        let (catalog, namespace, table_name, _temp_dir) = get_test_catalog_and_table().await;

        // Test creating a catalog-backed provider
        let provider =
            IcebergTableProvider::try_new(catalog.clone(), namespace.clone(), table_name.clone())
                .await
                .unwrap();

        // Verify the schema is loaded correctly
        let schema = provider.schema();
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "name");
    }

    #[tokio::test]
    async fn test_catalog_backed_provider_scan() {
        let (catalog, namespace, table_name, _temp_dir) = get_test_catalog_and_table().await;

        let provider =
            IcebergTableProvider::try_new(catalog.clone(), namespace.clone(), table_name.clone())
                .await
                .unwrap();

        let ctx = SessionContext::new();
        ctx.register_table("test_table", Arc::new(provider))
            .unwrap();

        // Test that scan operations work correctly
        let df = ctx.sql("SELECT * FROM test_table").await.unwrap();

        // Verify the schema in the query result
        let df_schema = df.schema();
        assert_eq!(df_schema.fields().len(), 2);
        assert_eq!(df_schema.field(0).name(), "id");
        assert_eq!(df_schema.field(1).name(), "name");

        let physical_plan = df.create_physical_plan().await;
        assert!(physical_plan.is_ok());
    }

    #[tokio::test]
    async fn test_catalog_backed_provider_insert() {
        let (catalog, namespace, table_name, _temp_dir) = get_test_catalog_and_table().await;

        let provider =
            IcebergTableProvider::try_new(catalog.clone(), namespace.clone(), table_name.clone())
                .await
                .unwrap();

        let ctx = SessionContext::new();
        ctx.register_table("test_table", Arc::new(provider))
            .unwrap();

        // Test that insert operations work correctly
        let result = ctx.sql("INSERT INTO test_table VALUES (1, 'test')").await;

        // Insert should succeed (or at least not fail during planning)
        assert!(result.is_ok());

        // Try to execute the insert plan
        let df = result.unwrap();
        let execution_result = df.collect().await;

        // The execution should succeed
        assert!(execution_result.is_ok());
    }

    #[tokio::test]
    async fn test_physical_input_schema_consistent_with_logical_input_schema() {
        let (catalog, namespace, table_name, _temp_dir) = get_test_catalog_and_table().await;

        let provider =
            IcebergTableProvider::try_new(catalog.clone(), namespace.clone(), table_name.clone())
                .await
                .unwrap();

        let ctx = SessionContext::new();
        ctx.register_table("test_table", Arc::new(provider))
            .unwrap();

        // Create a query plan
        let df = ctx.sql("SELECT id, name FROM test_table").await.unwrap();

        // Get logical schema before consuming df
        let logical_schema = df.schema().clone();

        // Get physical plan (this consumes df)
        let physical_plan = df.create_physical_plan().await.unwrap();
        let physical_schema = physical_plan.schema();

        // Verify that logical and physical schemas are consistent
        assert_eq!(
            logical_schema.fields().len(),
            physical_schema.fields().len()
        );

        for (logical_field, physical_field) in logical_schema
            .fields()
            .iter()
            .zip(physical_schema.fields().iter())
        {
            assert_eq!(logical_field.name(), physical_field.name());
            assert_eq!(logical_field.data_type(), physical_field.data_type());
        }
    }
}
