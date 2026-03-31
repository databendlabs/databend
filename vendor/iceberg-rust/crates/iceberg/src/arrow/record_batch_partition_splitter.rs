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

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{ArrayRef, BooleanArray, RecordBatch, StructArray};
use arrow_select::filter::filter_record_batch;

use super::arrow_struct_to_literal;
use super::partition_value_calculator::PartitionValueCalculator;
use crate::spec::{Literal, PartitionKey, PartitionSpecRef, SchemaRef, StructType};
use crate::{Error, ErrorKind, Result};

/// Column name for the projected partition values struct
pub const PROJECTED_PARTITION_VALUE_COLUMN: &str = "_partition";

/// The splitter used to split the record batch into multiple record batches by the partition spec.
/// 1. It will project and transform the input record batch based on the partition spec, get the partitioned record batch.
/// 2. Split the input record batch into multiple record batches based on the partitioned record batch.
///
/// # Partition Value Modes
///
/// The splitter supports two modes for obtaining partition values:
/// - **Computed mode** (`calculator` is `Some`): Computes partition values from source columns using transforms
/// - **Pre-computed mode** (`calculator` is `None`): Expects a `_partition` column in the input batch
pub struct RecordBatchPartitionSplitter {
    schema: SchemaRef,
    partition_spec: PartitionSpecRef,
    calculator: Option<PartitionValueCalculator>,
    partition_type: StructType,
}

impl RecordBatchPartitionSplitter {
    /// Create a new RecordBatchPartitionSplitter.
    ///
    /// # Arguments
    ///
    /// * `iceberg_schema` - The Iceberg schema reference
    /// * `partition_spec` - The partition specification reference
    /// * `calculator` - Optional calculator for computing partition values from source columns.
    ///   - `Some(calculator)`: Compute partition values from source columns using transforms
    ///   - `None`: Expect a pre-computed `_partition` column in the input batch
    ///
    /// # Returns
    ///
    /// Returns a new `RecordBatchPartitionSplitter` instance or an error if initialization fails.
    pub fn try_new(
        iceberg_schema: SchemaRef,
        partition_spec: PartitionSpecRef,
        calculator: Option<PartitionValueCalculator>,
    ) -> Result<Self> {
        let partition_type = partition_spec.partition_type(&iceberg_schema)?;

        Ok(Self {
            schema: iceberg_schema,
            partition_spec,
            calculator,
            partition_type,
        })
    }

    /// Create a new RecordBatchPartitionSplitter with computed partition values.
    ///
    /// This is a convenience method that creates a calculator and initializes the splitter
    /// to compute partition values from source columns.
    ///
    /// # Arguments
    ///
    /// * `iceberg_schema` - The Iceberg schema reference
    /// * `partition_spec` - The partition specification reference
    ///
    /// # Returns
    ///
    /// Returns a new `RecordBatchPartitionSplitter` instance or an error if initialization fails.
    pub fn try_new_with_computed_values(
        iceberg_schema: SchemaRef,
        partition_spec: PartitionSpecRef,
    ) -> Result<Self> {
        let calculator = PartitionValueCalculator::try_new(&partition_spec, &iceberg_schema)?;
        Self::try_new(iceberg_schema, partition_spec, Some(calculator))
    }

    /// Create a new RecordBatchPartitionSplitter expecting pre-computed partition values.
    ///
    /// This is a convenience method that initializes the splitter to expect a `_partition`
    /// column in the input batches.
    ///
    /// # Arguments
    ///
    /// * `iceberg_schema` - The Iceberg schema reference
    /// * `partition_spec` - The partition specification reference
    ///
    /// # Returns
    ///
    /// Returns a new `RecordBatchPartitionSplitter` instance or an error if initialization fails.
    pub fn try_new_with_precomputed_values(
        iceberg_schema: SchemaRef,
        partition_spec: PartitionSpecRef,
    ) -> Result<Self> {
        Self::try_new(iceberg_schema, partition_spec, None)
    }

    /// Split the record batch into multiple record batches based on the partition spec.
    pub fn split(&self, batch: &RecordBatch) -> Result<Vec<(PartitionKey, RecordBatch)>> {
        let partition_structs = if let Some(calculator) = &self.calculator {
            // Compute partition values from source columns using calculator
            let partition_array = calculator.calculate(batch)?;
            let struct_array = arrow_struct_to_literal(&partition_array, &self.partition_type)?;

            struct_array
                .into_iter()
                .map(|s| {
                    if let Some(Literal::Struct(s)) = s {
                        Ok(s)
                    } else {
                        Err(Error::new(
                            ErrorKind::DataInvalid,
                            "Partition value is not a struct literal or is null",
                        ))
                    }
                })
                .collect::<Result<Vec<_>>>()?
        } else {
            // Extract partition values from pre-computed partition column
            let partition_column = batch
                .column_by_name(PROJECTED_PARTITION_VALUE_COLUMN)
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "Partition column '{PROJECTED_PARTITION_VALUE_COLUMN}' not found in batch"
                        ),
                    )
                })?;

            let partition_struct_array = partition_column
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        "Partition column is not a StructArray",
                    )
                })?;

            let arrow_struct_array = Arc::new(partition_struct_array.clone()) as ArrayRef;
            let struct_array = arrow_struct_to_literal(&arrow_struct_array, &self.partition_type)?;

            struct_array
                .into_iter()
                .map(|s| {
                    if let Some(Literal::Struct(s)) = s {
                        Ok(s)
                    } else {
                        Err(Error::new(
                            ErrorKind::DataInvalid,
                            "Partition value is not a struct literal or is null",
                        ))
                    }
                })
                .collect::<Result<Vec<_>>>()?
        };

        // Group the batch by row value.
        let mut group_ids = HashMap::new();
        partition_structs
            .iter()
            .enumerate()
            .for_each(|(row_id, row)| {
                group_ids.entry(row.clone()).or_insert(vec![]).push(row_id);
            });

        // Partition the batch with same partition partition_values
        let mut partition_batches = Vec::with_capacity(group_ids.len());
        for (row, row_ids) in group_ids.into_iter() {
            // generate the bool filter array from column_ids
            let filter_array: BooleanArray = {
                let mut filter = vec![false; batch.num_rows()];
                row_ids.into_iter().for_each(|row_id| {
                    filter[row_id] = true;
                });
                filter.into()
            };

            // Create PartitionKey from the partition struct
            let partition_key = PartitionKey::new(
                self.partition_spec.as_ref().clone(),
                self.schema.clone(),
                row,
            );

            // filter the RecordBatch
            partition_batches.push((partition_key, filter_record_batch(batch, &filter_array)?));
        }

        Ok(partition_batches)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Int32Array, RecordBatch, StringArray};
    use arrow_schema::DataType;
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

    use super::*;
    use crate::arrow::schema_to_arrow_schema;
    use crate::spec::{
        NestedField, PartitionSpecBuilder, PrimitiveLiteral, Schema, Struct, Transform, Type,
        UnboundPartitionField,
    };

    #[test]
    fn test_record_batch_partition_split() {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::required(
                        1,
                        "id",
                        Type::Primitive(crate::spec::PrimitiveType::Int),
                    )
                    .into(),
                    NestedField::required(
                        2,
                        "name",
                        Type::Primitive(crate::spec::PrimitiveType::String),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );
        let partition_spec = Arc::new(
            PartitionSpecBuilder::new(schema.clone())
                .with_spec_id(1)
                .add_unbound_field(UnboundPartitionField {
                    source_id: 1,
                    field_id: None,
                    name: "id_bucket".to_string(),
                    transform: Transform::Identity,
                })
                .unwrap()
                .build()
                .unwrap(),
        );
        let partition_splitter = RecordBatchPartitionSplitter::try_new_with_computed_values(
            schema.clone(),
            partition_spec,
        )
        .expect("Failed to create splitter");

        let arrow_schema = Arc::new(schema_to_arrow_schema(&schema).unwrap());
        let id_array = Int32Array::from(vec![1, 2, 1, 3, 2, 3, 1]);
        let data_array = StringArray::from(vec!["a", "b", "c", "d", "e", "f", "g"]);
        let batch = RecordBatch::try_new(arrow_schema.clone(), vec![
            Arc::new(id_array),
            Arc::new(data_array),
        ])
        .expect("Failed to create RecordBatch");

        let mut partitioned_batches = partition_splitter
            .split(&batch)
            .expect("Failed to split RecordBatch");
        partitioned_batches.sort_by_key(|(partition_key, _)| {
            if let PrimitiveLiteral::Int(i) = partition_key.data().fields()[0]
                .as_ref()
                .unwrap()
                .as_primitive_literal()
                .unwrap()
            {
                i
            } else {
                panic!("The partition value is not a int");
            }
        });
        assert_eq!(partitioned_batches.len(), 3);
        {
            // check the first partition
            let expected_id_array = Int32Array::from(vec![1, 1, 1]);
            let expected_data_array = StringArray::from(vec!["a", "c", "g"]);
            let expected_batch = RecordBatch::try_new(arrow_schema.clone(), vec![
                Arc::new(expected_id_array),
                Arc::new(expected_data_array),
            ])
            .expect("Failed to create expected RecordBatch");
            assert_eq!(partitioned_batches[0].1, expected_batch);
        }
        {
            // check the second partition
            let expected_id_array = Int32Array::from(vec![2, 2]);
            let expected_data_array = StringArray::from(vec!["b", "e"]);
            let expected_batch = RecordBatch::try_new(arrow_schema.clone(), vec![
                Arc::new(expected_id_array),
                Arc::new(expected_data_array),
            ])
            .expect("Failed to create expected RecordBatch");
            assert_eq!(partitioned_batches[1].1, expected_batch);
        }
        {
            // check the third partition
            let expected_id_array = Int32Array::from(vec![3, 3]);
            let expected_data_array = StringArray::from(vec!["d", "f"]);
            let expected_batch = RecordBatch::try_new(arrow_schema.clone(), vec![
                Arc::new(expected_id_array),
                Arc::new(expected_data_array),
            ])
            .expect("Failed to create expected RecordBatch");
            assert_eq!(partitioned_batches[2].1, expected_batch);
        }

        let partition_values = partitioned_batches
            .iter()
            .map(|(partition_key, _)| partition_key.data().clone())
            .collect::<Vec<_>>();
        // check partition value is struct(1), struct(2), struct(3)
        assert_eq!(partition_values, vec![
            Struct::from_iter(vec![Some(Literal::int(1))]),
            Struct::from_iter(vec![Some(Literal::int(2))]),
            Struct::from_iter(vec![Some(Literal::int(3))]),
        ]);
    }

    #[test]
    fn test_record_batch_partition_split_with_partition_column() {
        use arrow_array::StructArray;
        use arrow_schema::{Field, Schema as ArrowSchema};

        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::required(
                        1,
                        "id",
                        Type::Primitive(crate::spec::PrimitiveType::Int),
                    )
                    .into(),
                    NestedField::required(
                        2,
                        "name",
                        Type::Primitive(crate::spec::PrimitiveType::String),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );
        let partition_spec = Arc::new(
            PartitionSpecBuilder::new(schema.clone())
                .with_spec_id(1)
                .add_unbound_field(UnboundPartitionField {
                    source_id: 1,
                    field_id: None,
                    name: "id_bucket".to_string(),
                    transform: Transform::Identity,
                })
                .unwrap()
                .build()
                .unwrap(),
        );

        // Create input schema with _partition column
        // Note: partition field IDs start from 1000 by default
        let partition_field = Field::new("id_bucket", DataType::Int32, false).with_metadata(
            HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), "1000".to_string())]),
        );
        let partition_struct_field = Field::new(
            PROJECTED_PARTITION_VALUE_COLUMN,
            DataType::Struct(vec![partition_field.clone()].into()),
            false,
        );

        let input_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            partition_struct_field,
        ]));

        // Create splitter expecting pre-computed partition column
        let partition_splitter = RecordBatchPartitionSplitter::try_new_with_precomputed_values(
            schema.clone(),
            partition_spec,
        )
        .expect("Failed to create splitter");

        // Create test data with pre-computed partition column
        let id_array = Int32Array::from(vec![1, 2, 1, 3, 2, 3, 1]);
        let data_array = StringArray::from(vec!["a", "b", "c", "d", "e", "f", "g"]);

        // Create partition column (same values as id for Identity transform)
        let partition_values = Int32Array::from(vec![1, 2, 1, 3, 2, 3, 1]);
        let partition_struct = StructArray::from(vec![(
            Arc::new(partition_field),
            Arc::new(partition_values) as ArrayRef,
        )]);

        let batch = RecordBatch::try_new(input_schema.clone(), vec![
            Arc::new(id_array),
            Arc::new(data_array),
            Arc::new(partition_struct),
        ])
        .expect("Failed to create RecordBatch");

        // Split using the pre-computed partition column
        let mut partitioned_batches = partition_splitter
            .split(&batch)
            .expect("Failed to split RecordBatch");

        partitioned_batches.sort_by_key(|(partition_key, _)| {
            if let PrimitiveLiteral::Int(i) = partition_key.data().fields()[0]
                .as_ref()
                .unwrap()
                .as_primitive_literal()
                .unwrap()
            {
                i
            } else {
                panic!("The partition value is not a int");
            }
        });

        assert_eq!(partitioned_batches.len(), 3);

        // Helper to extract id and name values from a batch
        let extract_values = |batch: &RecordBatch| -> (Vec<i32>, Vec<String>) {
            let id_col = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let name_col = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            (
                id_col.values().to_vec(),
                name_col.iter().map(|s| s.unwrap().to_string()).collect(),
            )
        };

        // Verify partition 1: id=1, names=["a", "c", "g"]
        let (key, batch) = &partitioned_batches[0];
        assert_eq!(key.data(), &Struct::from_iter(vec![Some(Literal::int(1))]));
        let (ids, names) = extract_values(batch);
        assert_eq!(ids, vec![1, 1, 1]);
        assert_eq!(names, vec!["a", "c", "g"]);

        // Verify partition 2: id=2, names=["b", "e"]
        let (key, batch) = &partitioned_batches[1];
        assert_eq!(key.data(), &Struct::from_iter(vec![Some(Literal::int(2))]));
        let (ids, names) = extract_values(batch);
        assert_eq!(ids, vec![2, 2]);
        assert_eq!(names, vec!["b", "e"]);

        // Verify partition 3: id=3, names=["d", "f"]
        let (key, batch) = &partitioned_batches[2];
        assert_eq!(key.data(), &Struct::from_iter(vec![Some(Literal::int(3))]));
        let (ids, names) = extract_values(batch);
        assert_eq!(ids, vec![3, 3]);
        assert_eq!(names, vec!["d", "f"]);
    }
}
