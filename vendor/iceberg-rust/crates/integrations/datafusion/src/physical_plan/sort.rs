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

//! Partition-based sorting for Iceberg tables.

use std::sync::Arc;

use datafusion::arrow::compute::SortOptions;
use datafusion::common::Result as DFResult;
use datafusion::error::DataFusionError;
use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::expressions::Column;
use datafusion::physical_plan::sorts::sort::SortExec;
use iceberg::arrow::PROJECTED_PARTITION_VALUE_COLUMN;

/// Sorts an ExecutionPlan by partition values for Iceberg tables.
///
/// This function takes an input ExecutionPlan that has been extended with partition values
/// (via `project_with_partition`) and returns a SortExec that sorts by the partition column.
/// The partition values are expected to be in a struct column named `PROJECTED_PARTITION_VALUE_COLUMN`.
///
/// For unpartitioned tables or plans without the partition column, returns an error.
///
/// # Arguments
/// * `input` - The input ExecutionPlan with projected partition values
///
/// # Returns
/// * `Ok(Arc<dyn ExecutionPlan>)` - A SortExec that sorts by partition values
/// * `Err` - If the partition column is not found
///
/// TODO remove dead_code mark when integrating with insert_into
#[allow(dead_code)]
pub(crate) fn sort_by_partition(input: Arc<dyn ExecutionPlan>) -> DFResult<Arc<dyn ExecutionPlan>> {
    let schema = input.schema();

    // Find the partition column in the schema
    let (partition_column_index, _partition_field) = schema
        .column_with_name(PROJECTED_PARTITION_VALUE_COLUMN)
        .ok_or_else(|| {
            DataFusionError::Plan(format!(
                "Partition column '{PROJECTED_PARTITION_VALUE_COLUMN}' not found in schema. Ensure the plan has been extended with partition values using project_with_partition."
            ))
        })?;

    // Create a single sort expression for the partition column
    let column_expr = Arc::new(Column::new(
        PROJECTED_PARTITION_VALUE_COLUMN,
        partition_column_index,
    ));

    let sort_expr = PhysicalSortExpr {
        expr: column_expr,
        options: SortOptions::default(), // Ascending, nulls last
    };

    // Create a SortExec with preserve_partitioning=true to ensure the output partitioning
    // is the same as the input partitioning, and the data is sorted within each partition
    let lex_ordering = LexOrdering::new(vec![sort_expr]).ok_or_else(|| {
        DataFusionError::Plan("Failed to create LexOrdering from sort expression".to_string())
    })?;

    let sort_exec = SortExec::new(lex_ordering, input).with_preserve_partitioning(true);

    Ok(Arc::new(sort_exec))
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{Int32Array, RecordBatch, StringArray, StructArray};
    use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema as ArrowSchema};
    use datafusion::datasource::{MemTable, TableProvider};
    use datafusion::prelude::SessionContext;

    use super::*;

    #[tokio::test]
    async fn test_sort_by_partition_basic() {
        // Create a schema with a partition column
        let partition_fields =
            Fields::from(vec![Field::new("id_partition", DataType::Int32, false)]);

        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new(
                PROJECTED_PARTITION_VALUE_COLUMN,
                DataType::Struct(partition_fields.clone()),
                false,
            ),
        ]));

        // Create test data with partition values
        let id_array = Arc::new(Int32Array::from(vec![3, 1, 2]));
        let name_array = Arc::new(StringArray::from(vec!["c", "a", "b"]));
        let partition_array = Arc::new(StructArray::from(vec![(
            Arc::new(Field::new("id_partition", DataType::Int32, false)),
            Arc::new(Int32Array::from(vec![3, 1, 2])) as _,
        )]));

        let batch =
            RecordBatch::try_new(schema.clone(), vec![id_array, name_array, partition_array])
                .unwrap();

        let ctx = SessionContext::new();
        let mem_table = MemTable::try_new(schema.clone(), vec![vec![batch]]).unwrap();
        let input = mem_table.scan(&ctx.state(), None, &[], None).await.unwrap();

        // Apply sort
        let sorted_plan = sort_by_partition(input).unwrap();

        // Execute and verify
        let result = datafusion::physical_plan::collect(sorted_plan, ctx.task_ctx())
            .await
            .unwrap();

        assert_eq!(result.len(), 1);
        let result_batch = &result[0];

        let id_col = result_batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        // Verify data is sorted by partition value
        assert_eq!(id_col.value(0), 1);
        assert_eq!(id_col.value(1), 2);
        assert_eq!(id_col.value(2), 3);
    }

    #[tokio::test]
    async fn test_sort_by_partition_missing_column() {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(schema.clone(), vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
        ])
        .unwrap();

        let ctx = SessionContext::new();
        let mem_table = MemTable::try_new(schema.clone(), vec![vec![batch]]).unwrap();
        let input = mem_table.scan(&ctx.state(), None, &[], None).await.unwrap();

        let result = sort_by_partition(input);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Partition column '_partition' not found")
        );
    }

    #[tokio::test]
    async fn test_sort_by_partition_multi_field() {
        // Test with multiple partition fields in the struct
        let partition_fields = Fields::from(vec![
            Field::new("year", DataType::Int32, false),
            Field::new("month", DataType::Int32, false),
        ]);

        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("data", DataType::Utf8, false),
            Field::new(
                PROJECTED_PARTITION_VALUE_COLUMN,
                DataType::Struct(partition_fields.clone()),
                false,
            ),
        ]));

        // Create test data with partition values (year, month)
        let id_array = Arc::new(Int32Array::from(vec![1, 2, 3, 4]));
        let data_array = Arc::new(StringArray::from(vec!["a", "b", "c", "d"]));

        // Partition values: (2024, 2), (2024, 1), (2023, 12), (2024, 1)
        let year_array = Arc::new(Int32Array::from(vec![2024, 2024, 2023, 2024]));
        let month_array = Arc::new(Int32Array::from(vec![2, 1, 12, 1]));

        let partition_array = Arc::new(StructArray::from(vec![
            (
                Arc::new(Field::new("year", DataType::Int32, false)),
                year_array as _,
            ),
            (
                Arc::new(Field::new("month", DataType::Int32, false)),
                month_array as _,
            ),
        ]));

        let batch =
            RecordBatch::try_new(schema.clone(), vec![id_array, data_array, partition_array])
                .unwrap();

        let ctx = SessionContext::new();
        let mem_table = MemTable::try_new(schema.clone(), vec![vec![batch]]).unwrap();
        let input = mem_table.scan(&ctx.state(), None, &[], None).await.unwrap();

        // Apply sort
        let sorted_plan = sort_by_partition(input).unwrap();

        // Execute and verify
        let result = datafusion::physical_plan::collect(sorted_plan, ctx.task_ctx())
            .await
            .unwrap();

        assert_eq!(result.len(), 1);
        let result_batch = &result[0];

        let id_col = result_batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        // Verify data is sorted by partition value (struct comparison)
        // Expected order: (2023, 12), (2024, 1), (2024, 1), (2024, 2)
        // Which corresponds to ids: 3, 2, 4, 1
        assert_eq!(id_col.value(0), 3);
        assert_eq!(id_col.value(1), 2);
        assert_eq!(id_col.value(2), 4);
        assert_eq!(id_col.value(3), 1);
    }
}
