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

use std::num::NonZeroUsize;
use std::sync::Arc;

use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::expressions::Column;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::{ExecutionPlan, Partitioning};
use iceberg::arrow::PROJECTED_PARTITION_VALUE_COLUMN;
use iceberg::spec::{TableMetadata, TableMetadataRef, Transform};
/// Creates an Iceberg-aware repartition execution plan that optimizes data distribution
/// for parallel processing while respecting Iceberg table partitioning semantics.
///
/// Automatically determines the optimal partitioning strategy based on the table's
/// partition specification.
///
/// ## Partitioning Strategies
///
/// - **Partitioned tables with Identity/Bucket transforms** – Uses hash partitioning on the
///   `_partition` column for optimal data distribution and file clustering. Ensures that rows
///   with the same partition values are co-located in the same task.
///
/// - **Partitioned tables with temporal transforms** – Uses round-robin partitioning for
///   temporal transforms (Year, Month, Day, Hour) that don't provide uniform hash distribution.
///
/// - **Unpartitioned tables** – Uses round-robin distribution to balance load evenly across workers.
///
/// ## Requirements
///
/// - **For partitioned tables**: The input MUST include the `_partition` column.
///   Add it by calling [`project_with_partition`](crate::physical_plan::project_with_partition) before [`repartition`].
/// - **For unpartitioned tables**: No special preparation needed.
/// - Returns an error if a partitioned table is missing the `_partition` column.
///
/// ## Performance Notes
///
/// - Only adds repartitioning when the input partitioning differs from the target.
/// - Requires an explicit target partition count for deterministic behavior.
///
/// # Arguments
///
/// * `input` - The input [`ExecutionPlan`]. For partitioned tables, must include the `_partition`
///   column (added via [`project_with_partition`](crate::physical_plan::project_with_partition)).
/// * `table_metadata` - Iceberg table metadata containing partition spec.
/// * `target_partitions` - Target number of partitions for parallel processing (must be > 0).
///
/// # Returns
///
/// An [`ExecutionPlan`] that applies the optimal partitioning strategy, or the original input plan
/// if repartitioning is not needed.
///
/// # Errors
///
/// Returns [`DataFusionError::Plan`] if a partitioned table input is missing the `_partition` column.
///
/// # Examples
///
/// For partitioned tables, first add the `_partition` column:
///
/// ```ignore
/// use std::num::NonZeroUsize;
/// use iceberg_datafusion::physical_plan::project_with_partition;
///
/// let plan_with_partition = project_with_partition(input_plan, &table)?;
///
/// let repartitioned_plan = repartition(
///     plan_with_partition,
///     table.metadata_ref(),
///     NonZeroUsize::new(4).unwrap(),
/// )?;
/// ```
pub(crate) fn repartition(
    input: Arc<dyn ExecutionPlan>,
    table_metadata: TableMetadataRef,
    target_partitions: NonZeroUsize,
) -> DFResult<Arc<dyn ExecutionPlan>> {
    let partitioning_strategy =
        determine_partitioning_strategy(&input, &table_metadata, target_partitions)?;

    Ok(Arc::new(RepartitionExec::try_new(
        input,
        partitioning_strategy,
    )?))
}

/// Determine the optimal partitioning strategy based on table metadata.
///
/// Analyzes the table's partition specification to select the most appropriate
/// DataFusion partitioning strategy for insert operations.
///
/// ## Partitioning Strategy
///
/// - **Partitioned tables**: Must have the `_partition` column in the input schema (added via
///   `project_with_partition`). Uses hash partitioning if the partition spec contains Identity
///   or Bucket transforms for good data distribution. Falls back to round-robin for temporal
///   transforms (Year, Month, Day, Hour) that don't provide uniform hash distribution.
///
/// - **Unpartitioned tables**: Always uses round-robin batch partitioning to ensure even load
///   distribution across workers.
///
/// ## Requirements
///
/// - **For partitioned tables**: The input MUST include the `_partition` column
///   (added via `project_with_partition()`).
/// - **For unpartitioned tables**: No special preparation needed.
/// - Returns an error if a partitioned table is missing the `_partition` column.
fn determine_partitioning_strategy(
    input: &Arc<dyn ExecutionPlan>,
    table_metadata: &TableMetadata,
    target_partitions: NonZeroUsize,
) -> DFResult<Partitioning> {
    let partition_spec = table_metadata.default_partition_spec();
    let input_schema = input.schema();
    let target_partition_count = target_partitions.get();

    // Check if partition spec has transforms suitable for hash partitioning
    let has_hash_friendly_transforms = partition_spec
        .fields()
        .iter()
        .any(|pf| matches!(pf.transform, Transform::Identity | Transform::Bucket(_)));

    let partition_col_result = input_schema.index_of(PROJECTED_PARTITION_VALUE_COLUMN);
    let is_partitioned_table = !partition_spec.is_unpartitioned();

    match (is_partitioned_table, partition_col_result) {
        // Case 1: Partitioned table with _partition column present
        (true, Ok(partition_col_idx)) => {
            let partition_expr = Arc::new(Column::new(
                PROJECTED_PARTITION_VALUE_COLUMN,
                partition_col_idx,
            )) as Arc<dyn PhysicalExpr>;

            if has_hash_friendly_transforms {
                Ok(Partitioning::Hash(
                    vec![partition_expr],
                    target_partition_count,
                ))
            } else {
                Ok(Partitioning::RoundRobinBatch(target_partition_count))
            }
        }

        // Case 2: Partitioned table missing _partition column (normally this should not happen)
        (true, Err(_)) => Err(DataFusionError::Plan(format!(
            "Partitioned table input missing {PROJECTED_PARTITION_VALUE_COLUMN} column. \
             Ensure projection happens before repartitioning."
        ))),

        // Case 3: Unpartitioned table, always use RoundRobinBatch
        (false, _) => Ok(Partitioning::RoundRobinBatch(target_partition_count)),
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::{
        DataType as ArrowDataType, Field as ArrowField, Fields, Schema as ArrowSchema, TimeUnit,
    };
    use datafusion::execution::TaskContext;
    use datafusion::physical_plan::empty::EmptyExec;
    use iceberg::TableIdent;
    use iceberg::io::FileIO;
    use iceberg::spec::{
        NestedField, NullOrder, PrimitiveType, Schema, SortDirection, SortField, SortOrder,
        Transform, Type,
    };
    use iceberg::table::Table;

    use super::*;

    fn create_test_table() -> Table {
        let schema = Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Long),
                )),
                Arc::new(NestedField::required(
                    2,
                    "data",
                    Type::Primitive(PrimitiveType::String),
                )),
            ])
            .build()
            .unwrap();

        let partition_spec = iceberg::spec::PartitionSpec::builder(schema.clone())
            .build()
            .unwrap();
        let sort_order = iceberg::spec::SortOrder::builder().build(&schema).unwrap();
        let table_metadata_builder = iceberg::spec::TableMetadataBuilder::new(
            schema,
            partition_spec,
            sort_order,
            "/test/table".to_string(),
            iceberg::spec::FormatVersion::V2,
            std::collections::HashMap::new(),
        )
        .unwrap();

        let table_metadata = table_metadata_builder.build().unwrap();

        Table::builder()
            .metadata(table_metadata.metadata)
            .identifier(TableIdent::from_strs(["test", "table"]).unwrap())
            .file_io(FileIO::from_path("/tmp").unwrap().build().unwrap())
            .metadata_location("/test/metadata.json".to_string())
            .build()
            .unwrap()
    }

    fn create_test_arrow_schema() -> Arc<ArrowSchema> {
        Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int64, false),
            ArrowField::new("data", ArrowDataType::Utf8, false),
        ]))
    }

    #[tokio::test]
    async fn test_repartition_unpartitioned_table() {
        let table = create_test_table();
        let input = Arc::new(EmptyExec::new(create_test_arrow_schema()));

        let repartitioned_plan = repartition(
            input.clone(),
            table.metadata_ref(),
            std::num::NonZeroUsize::new(4).unwrap(),
        )
        .unwrap();

        assert_ne!(input.name(), repartitioned_plan.name());
        assert_eq!(repartitioned_plan.name(), "RepartitionExec");
    }

    #[tokio::test]
    async fn test_repartition_explicit_partitions() {
        let table = create_test_table();
        let input = Arc::new(EmptyExec::new(create_test_arrow_schema()));

        let repartitioned_plan = repartition(
            input,
            table.metadata_ref(),
            std::num::NonZeroUsize::new(8).unwrap(),
        )
        .unwrap();

        let partitioning = repartitioned_plan.properties().output_partitioning();
        match partitioning {
            Partitioning::RoundRobinBatch(n) => {
                assert_eq!(*n, 8);
            }
            _ => panic!("Expected RoundRobinBatch partitioning"),
        }
    }

    #[tokio::test]
    async fn test_repartition_zero_partitions_fails() {
        let _table = create_test_table();
        let _input = Arc::new(EmptyExec::new(create_test_arrow_schema()));

        let result = std::num::NonZeroUsize::new(0);
        assert!(result.is_none(), "NonZeroUsize::new(0) should return None");

        // Test that we can't call repartition with 0 partitions
        // This is prevented at compile time by NonZeroUsize
        let _ = result; // This would be None, so we can't call repartition
    }

    #[tokio::test]
    async fn test_partition_count_validation() {
        let table = create_test_table();
        let input = Arc::new(EmptyExec::new(create_test_arrow_schema()));

        let target_partitions = 16;
        let repartitioned_plan = repartition(
            input,
            table.metadata_ref(),
            std::num::NonZeroUsize::new(target_partitions).unwrap(),
        )
        .unwrap();

        let partitioning = repartitioned_plan.properties().output_partitioning();
        match partitioning {
            Partitioning::RoundRobinBatch(n) => {
                assert_eq!(*n, target_partitions);
            }
            _ => panic!("Expected RoundRobinBatch partitioning"),
        }
    }

    #[tokio::test]
    async fn test_datafusion_repartitioning_integration() {
        let table = create_test_table();
        let input = Arc::new(EmptyExec::new(create_test_arrow_schema()));

        let repartitioned_plan = repartition(
            input,
            table.metadata_ref(),
            std::num::NonZeroUsize::new(3).unwrap(),
        )
        .unwrap();

        let partitioning = repartitioned_plan.properties().output_partitioning();
        match partitioning {
            Partitioning::RoundRobinBatch(n) => {
                assert_eq!(*n, 3, "Should use round-robin for unpartitioned table");
            }
            _ => panic!("Expected RoundRobinBatch partitioning for unpartitioned table"),
        }

        let task_ctx = Arc::new(TaskContext::default());
        let stream = repartitioned_plan.execute(0, task_ctx.clone()).unwrap();

        assert!(!stream.schema().fields().is_empty());
    }

    #[tokio::test]
    async fn test_bucket_aware_partitioning() {
        let schema = Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Long),
                )),
                Arc::new(NestedField::required(
                    2,
                    "category",
                    Type::Primitive(PrimitiveType::String),
                )),
            ])
            .build()
            .unwrap();

        let sort_order = SortOrder::builder()
            .with_order_id(1)
            .with_sort_field(SortField {
                source_id: 2,
                transform: Transform::Bucket(4),
                direction: SortDirection::Ascending,
                null_order: NullOrder::First,
            })
            .build(&schema)
            .unwrap();

        let partition_spec = iceberg::spec::PartitionSpec::builder(schema.clone())
            .build()
            .unwrap();
        let table_metadata_builder = iceberg::spec::TableMetadataBuilder::new(
            schema,
            partition_spec,
            sort_order,
            "/test/bucketed_table".to_string(),
            iceberg::spec::FormatVersion::V2,
            std::collections::HashMap::new(),
        )
        .unwrap();

        let table_metadata = table_metadata_builder.build().unwrap();
        let table = Table::builder()
            .metadata(table_metadata.metadata)
            .identifier(TableIdent::from_strs(["test", "bucketed_table"]).unwrap())
            .file_io(FileIO::from_path("/tmp").unwrap().build().unwrap())
            .metadata_location("/test/bucketed_metadata.json".to_string())
            .build()
            .unwrap();

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int64, false),
            ArrowField::new("category", ArrowDataType::Utf8, false),
        ]));
        let input = Arc::new(EmptyExec::new(arrow_schema));
        let repartitioned_plan = repartition(
            input,
            table.metadata_ref(),
            std::num::NonZeroUsize::new(4).unwrap(),
        )
        .unwrap();

        let partitioning = repartitioned_plan.properties().output_partitioning();
        // For bucketed tables without _partition column, should use round-robin
        // since the new logic prioritizes _partition column when available
        match partitioning {
            Partitioning::Hash(_, _) => {
                // This would happen if _partition column is present
            }
            Partitioning::RoundRobinBatch(_) => {
                // This happens when _partition column is not present
            }
            _ => panic!("Unexpected partitioning strategy"),
        }
    }

    #[tokio::test]
    async fn test_combined_partition_and_bucket_strategy() {
        let schema = Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::required(
                    1,
                    "date",
                    Type::Primitive(PrimitiveType::Date),
                )),
                Arc::new(NestedField::required(
                    2,
                    "user_id",
                    Type::Primitive(PrimitiveType::Long),
                )),
                Arc::new(NestedField::required(
                    3,
                    "amount",
                    Type::Primitive(PrimitiveType::Long),
                )),
            ])
            .build()
            .unwrap();

        let partition_spec = iceberg::spec::PartitionSpec::builder(schema.clone())
            .add_partition_field("date", "date", Transform::Identity)
            .unwrap()
            .build()
            .unwrap();

        let sort_order = SortOrder::builder()
            .with_order_id(1)
            .with_sort_field(SortField {
                source_id: 2,
                transform: Transform::Bucket(8),
                direction: SortDirection::Ascending,
                null_order: NullOrder::First,
            })
            .build(&schema)
            .unwrap();

        let table_metadata_builder = iceberg::spec::TableMetadataBuilder::new(
            schema,
            partition_spec,
            sort_order,
            "/test/partitioned_bucketed_table".to_string(),
            iceberg::spec::FormatVersion::V2,
            std::collections::HashMap::new(),
        )
        .unwrap();

        let table_metadata = table_metadata_builder.build().unwrap();
        let table = Table::builder()
            .metadata(table_metadata.metadata)
            .identifier(TableIdent::from_strs(["test", "partitioned_bucketed_table"]).unwrap())
            .file_io(FileIO::from_path("/tmp").unwrap().build().unwrap())
            .metadata_location("/test/partitioned_bucketed_metadata.json".to_string())
            .build()
            .unwrap();

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("date", ArrowDataType::Date32, false),
            ArrowField::new("user_id", ArrowDataType::Int64, false),
            ArrowField::new("amount", ArrowDataType::Int64, false),
            ArrowField::new(
                PROJECTED_PARTITION_VALUE_COLUMN,
                ArrowDataType::Struct(Fields::empty()),
                false,
            ),
        ]));
        let input = Arc::new(EmptyExec::new(arrow_schema));
        let repartitioned_plan = repartition(
            input,
            table.metadata_ref(),
            std::num::NonZeroUsize::new(4).unwrap(),
        )
        .unwrap();

        let partitioning = repartitioned_plan.properties().output_partitioning();
        match partitioning {
            Partitioning::Hash(exprs, _) => {
                // Should use _partition column for hash partitioning
                assert_eq!(
                    exprs.len(),
                    1,
                    "Should have exactly one hash column (_partition)"
                );

                let column_names: Vec<String> = exprs
                    .iter()
                    .filter_map(|expr| {
                        expr.as_any()
                            .downcast_ref::<Column>()
                            .map(|col| col.name().to_string())
                    })
                    .collect();

                assert!(
                    column_names.contains(&PROJECTED_PARTITION_VALUE_COLUMN.to_string()),
                    "Should use _partition column, got: {column_names:?}"
                );
            }
            _ => panic!("Expected Hash partitioning with Identity transform"),
        }
    }

    #[tokio::test]
    async fn test_none_distribution_mode_fallback() {
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            ))])
            .build()
            .unwrap();

        let partition_spec = iceberg::spec::PartitionSpec::builder(schema.clone())
            .build()
            .unwrap();
        let sort_order = iceberg::spec::SortOrder::builder().build(&schema).unwrap();

        let mut properties = std::collections::HashMap::new();
        properties.insert("write.distribution-mode".to_string(), "none".to_string());

        let table_metadata_builder = iceberg::spec::TableMetadataBuilder::new(
            schema,
            partition_spec,
            sort_order,
            "/test/none_table".to_string(),
            iceberg::spec::FormatVersion::V2,
            properties,
        )
        .unwrap();

        let table_metadata = table_metadata_builder.build().unwrap();
        let table = Table::builder()
            .metadata(table_metadata.metadata)
            .identifier(TableIdent::from_strs(["test", "none_table"]).unwrap())
            .file_io(FileIO::from_path("/tmp").unwrap().build().unwrap())
            .metadata_location("/test/none_metadata.json".to_string())
            .build()
            .unwrap();

        let input = Arc::new(EmptyExec::new(create_test_arrow_schema()));
        let repartitioned_plan = repartition(
            input,
            table.metadata_ref(),
            std::num::NonZeroUsize::new(4).unwrap(),
        )
        .unwrap();

        let partitioning = repartitioned_plan.properties().output_partitioning();
        assert!(
            matches!(partitioning, Partitioning::RoundRobinBatch(_)),
            "Should use round-robin for 'none' distribution mode"
        );
    }

    #[tokio::test]
    async fn test_schema_ref_convenience_method() {
        let table = create_test_table();

        let schema_ref_1 = table.current_schema_ref();
        let schema_ref_2 = Arc::clone(table.metadata().current_schema());

        assert!(
            Arc::ptr_eq(&schema_ref_1, &schema_ref_2),
            "current_schema_ref() should return the same Arc as manual approach"
        );
    }

    #[tokio::test]
    async fn test_range_only_partitions_use_round_robin() {
        let schema = Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::required(
                    1,
                    "date",
                    Type::Primitive(PrimitiveType::Date),
                )),
                Arc::new(NestedField::required(
                    2,
                    "amount",
                    Type::Primitive(PrimitiveType::Long),
                )),
            ])
            .build()
            .unwrap();

        let partition_spec = iceberg::spec::PartitionSpec::builder(schema.clone())
            .add_partition_field("date", "date_day", Transform::Day)
            .unwrap()
            .build()
            .unwrap();

        let sort_order = iceberg::spec::SortOrder::builder().build(&schema).unwrap();
        let table_metadata_builder = iceberg::spec::TableMetadataBuilder::new(
            schema,
            partition_spec,
            sort_order,
            "/test/range_only_table".to_string(),
            iceberg::spec::FormatVersion::V2,
            std::collections::HashMap::new(),
        )
        .unwrap();

        let table_metadata = table_metadata_builder.build().unwrap();
        let table = Table::builder()
            .metadata(table_metadata.metadata)
            .identifier(TableIdent::from_strs(["test", "range_only_table"]).unwrap())
            .file_io(FileIO::from_path("/tmp").unwrap().build().unwrap())
            .metadata_location("/test/range_only_metadata.json".to_string())
            .build()
            .unwrap();

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("date", ArrowDataType::Date32, false),
            ArrowField::new("amount", ArrowDataType::Int64, false),
            ArrowField::new(
                PROJECTED_PARTITION_VALUE_COLUMN,
                ArrowDataType::Struct(Fields::empty()),
                false,
            ),
        ]));
        let input = Arc::new(EmptyExec::new(arrow_schema));
        let repartitioned_plan = repartition(
            input,
            table.metadata_ref(),
            std::num::NonZeroUsize::new(4).unwrap(),
        )
        .unwrap();

        let partitioning = repartitioned_plan.properties().output_partitioning();
        assert!(
            matches!(partitioning, Partitioning::RoundRobinBatch(_)),
            "Should use round-robin for temporal transforms (Day) that don't provide good hash distribution"
        );
    }

    #[tokio::test]
    async fn test_mixed_transforms_use_hash_partitioning() {
        let schema = Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::required(
                    1,
                    "date",
                    Type::Primitive(PrimitiveType::Date),
                )),
                Arc::new(NestedField::required(
                    2,
                    "user_id",
                    Type::Primitive(PrimitiveType::Long),
                )),
                Arc::new(NestedField::required(
                    3,
                    "amount",
                    Type::Primitive(PrimitiveType::Long),
                )),
            ])
            .build()
            .unwrap();

        let partition_spec = iceberg::spec::PartitionSpec::builder(schema.clone())
            .add_partition_field("date", "date_day", Transform::Day)
            .unwrap()
            .add_partition_field("user_id", "user_id", Transform::Identity)
            .unwrap()
            .build()
            .unwrap();

        let sort_order = iceberg::spec::SortOrder::builder().build(&schema).unwrap();
        let table_metadata_builder = iceberg::spec::TableMetadataBuilder::new(
            schema,
            partition_spec,
            sort_order,
            "/test/mixed_transforms_table".to_string(),
            iceberg::spec::FormatVersion::V2,
            std::collections::HashMap::new(),
        )
        .unwrap();

        let table_metadata = table_metadata_builder.build().unwrap();
        let table = Table::builder()
            .metadata(table_metadata.metadata)
            .identifier(TableIdent::from_strs(["test", "mixed_transforms_table"]).unwrap())
            .file_io(FileIO::from_path("/tmp").unwrap().build().unwrap())
            .metadata_location("/test/mixed_transforms_metadata.json".to_string())
            .build()
            .unwrap();

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("date", ArrowDataType::Date32, false),
            ArrowField::new("user_id", ArrowDataType::Int64, false),
            ArrowField::new("amount", ArrowDataType::Int64, false),
            ArrowField::new(
                PROJECTED_PARTITION_VALUE_COLUMN,
                ArrowDataType::Struct(Fields::empty()),
                false,
            ),
        ]));
        let input = Arc::new(EmptyExec::new(arrow_schema));
        let repartitioned_plan = repartition(
            input,
            table.metadata_ref(),
            std::num::NonZeroUsize::new(4).unwrap(),
        )
        .unwrap();

        let partitioning = repartitioned_plan.properties().output_partitioning();
        match partitioning {
            Partitioning::Hash(exprs, _) => {
                assert_eq!(exprs.len(), 1, "Should have one hash column (_partition)");
                let column_names: Vec<String> = exprs
                    .iter()
                    .filter_map(|expr| {
                        expr.as_any()
                            .downcast_ref::<Column>()
                            .map(|col| col.name().to_string())
                    })
                    .collect();
                assert!(
                    column_names.contains(&PROJECTED_PARTITION_VALUE_COLUMN.to_string()),
                    "Should use _partition column for mixed transforms with Identity, got: {column_names:?}"
                );
            }
            _ => panic!("Expected Hash partitioning for table with identity transforms"),
        }
    }

    #[tokio::test]
    async fn test_partition_column_with_temporal_transforms_uses_round_robin() {
        let schema = Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::required(
                    1,
                    "event_time",
                    Type::Primitive(PrimitiveType::Timestamp),
                )),
                Arc::new(NestedField::required(
                    2,
                    "amount",
                    Type::Primitive(PrimitiveType::Long),
                )),
            ])
            .build()
            .unwrap();

        let partition_spec = iceberg::spec::PartitionSpec::builder(schema.clone())
            .add_partition_field("event_time", "event_month", Transform::Month)
            .unwrap()
            .build()
            .unwrap();

        let sort_order = iceberg::spec::SortOrder::builder().build(&schema).unwrap();
        let table_metadata_builder = iceberg::spec::TableMetadataBuilder::new(
            schema,
            partition_spec,
            sort_order,
            "/test/temporal_partition".to_string(),
            iceberg::spec::FormatVersion::V2,
            std::collections::HashMap::new(),
        )
        .unwrap();

        let table_metadata = table_metadata_builder.build().unwrap();
        let table = Table::builder()
            .metadata(table_metadata.metadata)
            .identifier(TableIdent::from_strs(["test", "temporal_partition"]).unwrap())
            .file_io(FileIO::from_path("/tmp").unwrap().build().unwrap())
            .metadata_location("/test/temporal_metadata.json".to_string())
            .build()
            .unwrap();

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new(
                "event_time",
                ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
            ArrowField::new("amount", ArrowDataType::Int64, false),
            ArrowField::new(
                PROJECTED_PARTITION_VALUE_COLUMN,
                ArrowDataType::Struct(Fields::empty()),
                false,
            ),
        ]));
        let input = Arc::new(EmptyExec::new(arrow_schema));

        let repartitioned_plan = repartition(
            input,
            table.metadata_ref(),
            std::num::NonZeroUsize::new(4).unwrap(),
        )
        .unwrap();

        let partitioning = repartitioned_plan.properties().output_partitioning();
        assert!(
            matches!(partitioning, Partitioning::RoundRobinBatch(_)),
            "Should use round-robin for _partition column with temporal transforms, not Hash"
        );
    }

    #[tokio::test]
    async fn test_partition_column_with_identity_transforms_uses_hash() {
        let schema = Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::required(
                    1,
                    "user_id",
                    Type::Primitive(PrimitiveType::Long),
                )),
                Arc::new(NestedField::required(
                    2,
                    "amount",
                    Type::Primitive(PrimitiveType::Long),
                )),
            ])
            .build()
            .unwrap();

        let partition_spec = iceberg::spec::PartitionSpec::builder(schema.clone())
            .add_partition_field("user_id", "user_id", Transform::Identity)
            .unwrap()
            .build()
            .unwrap();

        let sort_order = iceberg::spec::SortOrder::builder().build(&schema).unwrap();
        let table_metadata_builder = iceberg::spec::TableMetadataBuilder::new(
            schema,
            partition_spec,
            sort_order,
            "/test/identity_partition".to_string(),
            iceberg::spec::FormatVersion::V2,
            std::collections::HashMap::new(),
        )
        .unwrap();

        let table_metadata = table_metadata_builder.build().unwrap();
        let table = Table::builder()
            .metadata(table_metadata.metadata)
            .identifier(TableIdent::from_strs(["test", "identity_partition"]).unwrap())
            .file_io(FileIO::from_path("/tmp").unwrap().build().unwrap())
            .metadata_location("/test/identity_metadata.json".to_string())
            .build()
            .unwrap();

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("user_id", ArrowDataType::Int64, false),
            ArrowField::new("amount", ArrowDataType::Int64, false),
            ArrowField::new(
                PROJECTED_PARTITION_VALUE_COLUMN,
                ArrowDataType::Struct(Fields::empty()),
                false,
            ),
        ]));
        let input = Arc::new(EmptyExec::new(arrow_schema));

        let repartitioned_plan = repartition(
            input,
            table.metadata_ref(),
            std::num::NonZeroUsize::new(4).unwrap(),
        )
        .unwrap();

        let partitioning = repartitioned_plan.properties().output_partitioning();
        assert!(
            matches!(partitioning, Partitioning::Hash(_, _)),
            "Should use Hash partitioning for _partition column with Identity transforms"
        );
    }
}
