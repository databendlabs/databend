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

//! Partition value projection for Iceberg tables.

use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{DataType, Schema as ArrowSchema};
use datafusion::common::Result as DFResult;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::{ColumnarValue, ExecutionPlan};
use iceberg::arrow::{PROJECTED_PARTITION_VALUE_COLUMN, PartitionValueCalculator};
use iceberg::spec::PartitionSpec;
use iceberg::table::Table;

use crate::to_datafusion_error;

/// Extends an ExecutionPlan with partition value calculations for Iceberg tables.
///
/// This function takes an input ExecutionPlan and extends it with an additional column
/// containing calculated partition values based on the table's partition specification.
/// For unpartitioned tables, returns the original plan unchanged.
///
/// # Arguments
/// * `input` - The input ExecutionPlan to extend
/// * `table` - The Iceberg table with partition specification
///
/// # Returns
/// * `Ok(Arc<dyn ExecutionPlan>)` - Extended plan with partition values column
/// * `Err` - If partition spec is not found or transformation fails
pub fn project_with_partition(
    input: Arc<dyn ExecutionPlan>,
    table: &Table,
) -> DFResult<Arc<dyn ExecutionPlan>> {
    let metadata = table.metadata();
    let partition_spec = metadata.default_partition_spec();
    let table_schema = metadata.current_schema();

    if partition_spec.is_unpartitioned() {
        return Ok(input);
    }

    let input_schema = input.schema();
    // TODO: Validate that input_schema matches the Iceberg table schema.
    // See: https://github.com/apache/iceberg-rust/issues/1752
    let calculator =
        PartitionValueCalculator::try_new(partition_spec.as_ref(), table_schema.as_ref())
            .map_err(to_datafusion_error)?;

    let mut projection_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> =
        Vec::with_capacity(input_schema.fields().len() + 1);

    for (index, field) in input_schema.fields().iter().enumerate() {
        let column_expr = Arc::new(Column::new(field.name(), index));
        projection_exprs.push((column_expr, field.name().clone()));
    }

    let partition_expr = Arc::new(PartitionExpr::new(calculator, partition_spec.clone()));
    projection_exprs.push((partition_expr, PROJECTED_PARTITION_VALUE_COLUMN.to_string()));

    let projection = ProjectionExec::try_new(projection_exprs, input)?;
    Ok(Arc::new(projection))
}

/// PhysicalExpr implementation for partition value calculation
#[derive(Debug, Clone)]
struct PartitionExpr {
    calculator: Arc<PartitionValueCalculator>,
    partition_spec: Arc<PartitionSpec>,
}

impl PartitionExpr {
    fn new(calculator: PartitionValueCalculator, partition_spec: Arc<PartitionSpec>) -> Self {
        Self {
            calculator: Arc::new(calculator),
            partition_spec,
        }
    }
}

// Manual PartialEq/Eq implementations for pointer-based equality
// (two PartitionExpr are equal if they share the same calculator and partition_spec instances)
impl PartialEq for PartitionExpr {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.calculator, &other.calculator)
            && Arc::ptr_eq(&self.partition_spec, &other.partition_spec)
    }
}

impl Eq for PartitionExpr {}

impl PhysicalExpr for PartitionExpr {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn data_type(&self, _input_schema: &ArrowSchema) -> DFResult<DataType> {
        Ok(self.calculator.partition_arrow_type().clone())
    }

    fn nullable(&self, _input_schema: &ArrowSchema) -> DFResult<bool> {
        Ok(false)
    }

    fn evaluate(&self, batch: &RecordBatch) -> DFResult<ColumnarValue> {
        let array = self
            .calculator
            .calculate(batch)
            .map_err(to_datafusion_error)?;
        Ok(ColumnarValue::Array(array))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> DFResult<Arc<dyn PhysicalExpr>> {
        Ok(self)
    }

    fn fmt_sql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let field_names: Vec<String> = self
            .partition_spec
            .fields()
            .iter()
            .map(|pf| format!("{}({})", pf.transform, pf.name))
            .collect();
        write!(f, "iceberg_partition_values[{}]", field_names.join(", "))
    }
}

impl std::fmt::Display for PartitionExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let field_names: Vec<&str> = self
            .partition_spec
            .fields()
            .iter()
            .map(|pf| pf.name.as_str())
            .collect();
        write!(f, "iceberg_partition_values({})", field_names.join(", "))
    }
}

impl std::hash::Hash for PartitionExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        // Two PartitionExpr are equal if they share the same calculator and partition_spec Arcs
        Arc::as_ptr(&self.calculator).hash(state);
        Arc::as_ptr(&self.partition_spec).hash(state);
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{ArrayRef, Int32Array, StructArray};
    use datafusion::arrow::datatypes::{Field, Fields};
    use datafusion::physical_plan::empty::EmptyExec;
    use iceberg::spec::{NestedField, PrimitiveType, Schema, StructType, Transform, Type};

    use super::*;

    #[test]
    fn test_partition_calculator_basic() {
        let table_schema = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .unwrap();

        let partition_spec = iceberg::spec::PartitionSpec::builder(Arc::new(table_schema.clone()))
            .add_partition_field("id", "id_partition", Transform::Identity)
            .unwrap()
            .build()
            .unwrap();

        let calculator = PartitionValueCalculator::try_new(&partition_spec, &table_schema).unwrap();

        // Verify partition type
        assert_eq!(calculator.partition_type().fields().len(), 1);
        assert_eq!(calculator.partition_type().fields()[0].name, "id_partition");
    }

    #[test]
    fn test_partition_expr_with_projection() {
        let table_schema = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .unwrap();

        let partition_spec = Arc::new(
            iceberg::spec::PartitionSpec::builder(Arc::new(table_schema.clone()))
                .add_partition_field("id", "id_partition", Transform::Identity)
                .unwrap()
                .build()
                .unwrap(),
        );

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let input = Arc::new(EmptyExec::new(arrow_schema.clone()));

        let calculator = PartitionValueCalculator::try_new(&partition_spec, &table_schema).unwrap();

        let mut projection_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> =
            Vec::with_capacity(arrow_schema.fields().len() + 1);
        for (i, field) in arrow_schema.fields().iter().enumerate() {
            let column_expr = Arc::new(Column::new(field.name(), i));
            projection_exprs.push((column_expr, field.name().clone()));
        }

        let partition_expr = Arc::new(PartitionExpr::new(calculator, partition_spec));
        projection_exprs.push((partition_expr, PROJECTED_PARTITION_VALUE_COLUMN.to_string()));

        let projection = ProjectionExec::try_new(projection_exprs, input).unwrap();
        let result = Arc::new(projection);

        assert_eq!(result.schema().fields().len(), 3);
        assert_eq!(result.schema().field(0).name(), "id");
        assert_eq!(result.schema().field(1).name(), "name");
        assert_eq!(result.schema().field(2).name(), "_partition");
    }

    #[test]
    fn test_partition_expr_evaluate() {
        let table_schema = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::required(2, "data", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .unwrap();

        let partition_spec = iceberg::spec::PartitionSpec::builder(Arc::new(table_schema.clone()))
            .add_partition_field("id", "id_partition", Transform::Identity)
            .unwrap()
            .build()
            .unwrap();

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("data", DataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(arrow_schema.clone(), vec![
            Arc::new(Int32Array::from(vec![10, 20, 30])),
            Arc::new(datafusion::arrow::array::StringArray::from(vec![
                "a", "b", "c",
            ])),
        ])
        .unwrap();

        let partition_spec = Arc::new(partition_spec);
        let calculator = PartitionValueCalculator::try_new(&partition_spec, &table_schema).unwrap();
        let partition_type = calculator.partition_arrow_type().clone();
        let expr = PartitionExpr::new(calculator, partition_spec);

        assert_eq!(expr.data_type(&arrow_schema).unwrap(), partition_type);
        assert!(!expr.nullable(&arrow_schema).unwrap());

        let result = expr.evaluate(&batch).unwrap();
        match result {
            ColumnarValue::Array(array) => {
                let struct_array = array.as_any().downcast_ref::<StructArray>().unwrap();
                let id_partition = struct_array
                    .column_by_name("id_partition")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                assert_eq!(id_partition.value(0), 10);
                assert_eq!(id_partition.value(1), 20);
                assert_eq!(id_partition.value(2), 30);
            }
            _ => panic!("Expected array result"),
        }
    }

    #[test]
    fn test_nested_partition() {
        let address_struct = StructType::new(vec![
            NestedField::required(3, "street", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::required(4, "city", Type::Primitive(PrimitiveType::String)).into(),
        ]);

        let table_schema = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::required(2, "address", Type::Struct(address_struct)).into(),
            ])
            .build()
            .unwrap();

        let partition_spec = iceberg::spec::PartitionSpec::builder(Arc::new(table_schema.clone()))
            .add_partition_field("address.city", "city_partition", Transform::Identity)
            .unwrap()
            .build()
            .unwrap();

        let struct_fields = Fields::from(vec![
            Field::new("street", DataType::Utf8, false),
            Field::new("city", DataType::Utf8, false),
        ]);

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("address", DataType::Struct(struct_fields), false),
        ]));

        let street_array = Arc::new(datafusion::arrow::array::StringArray::from(vec![
            "123 Main St",
            "456 Oak Ave",
        ]));
        let city_array = Arc::new(datafusion::arrow::array::StringArray::from(vec![
            "New York",
            "Los Angeles",
        ]));

        let struct_array = StructArray::from(vec![
            (
                Arc::new(Field::new("street", DataType::Utf8, false)),
                street_array as ArrayRef,
            ),
            (
                Arc::new(Field::new("city", DataType::Utf8, false)),
                city_array as ArrayRef,
            ),
        ]);

        let batch = RecordBatch::try_new(arrow_schema.clone(), vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(struct_array),
        ])
        .unwrap();

        let calculator = PartitionValueCalculator::try_new(&partition_spec, &table_schema).unwrap();
        let array = calculator.calculate(&batch).unwrap();

        let struct_array = array.as_any().downcast_ref::<StructArray>().unwrap();
        let city_partition = struct_array
            .column_by_name("city_partition")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();

        assert_eq!(city_partition.value(0), "New York");
        assert_eq!(city_partition.value(1), "Los Angeles");
    }
}
