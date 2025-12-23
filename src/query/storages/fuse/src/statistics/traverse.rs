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

use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::ColumnId;
use databend_common_expression::Scalar;
use databend_common_expression::TableField;
use databend_common_expression::Value;
use databend_common_expression::types::AccessType;
use databend_common_expression::types::AnyType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::map::KvPair;
use databend_storages_common_index::Index;
use databend_storages_common_index::RangeIndex;

pub type TraverseResult =
    databend_common_exception::Result<Vec<(ColumnId, Value<AnyType>, DataType)>>;

// traverses columns and collects the leaves in depth first manner
pub fn traverse_values_dfs(columns: &[BlockEntry], fields: &[TableField]) -> TraverseResult {
    let mut leaves = vec![];
    for (entry, field) in columns.iter().zip(fields) {
        let mut next_column_id = field.column_id;
        match entry {
            BlockEntry::Const(s, data_type, _) => {
                traverse_scalar_recursive(s, data_type, &mut next_column_id, &mut leaves)?;
            }
            BlockEntry::Column(c) => {
                traverse_column_recursive(c, &c.data_type(), &mut next_column_id, &mut leaves)?;
            }
        }
    }
    Ok(leaves)
}

fn traverse_column_recursive(
    column: &Column,
    data_type: &DataType,
    next_column_id: &mut ColumnId,
    leaves: &mut Vec<(ColumnId, Value<AnyType>, DataType)>,
) -> databend_common_exception::Result<()> {
    match data_type.remove_nullable() {
        DataType::Tuple(inner_types) => {
            let inner_columns = if data_type.is_nullable() {
                let nullable_column = column.as_nullable().unwrap();
                nullable_column.column.as_tuple().unwrap()
            } else {
                column.as_tuple().unwrap()
            };
            for (inner_column, inner_type) in inner_columns.iter().zip(inner_types.iter()) {
                traverse_column_recursive(inner_column, inner_type, next_column_id, leaves)?;
            }
        }
        DataType::Array(inner_type) => {
            let array_column = if data_type.is_nullable() {
                let nullable_column = column.as_nullable().unwrap();
                nullable_column.column.as_array().unwrap()
            } else {
                column.as_array().unwrap()
            };
            traverse_column_recursive(
                &array_column.underlying_column(),
                &inner_type,
                next_column_id,
                leaves,
            )?;
        }
        DataType::Map(inner_type) => match *inner_type {
            DataType::Tuple(inner_types) => {
                let map_column = if data_type.is_nullable() {
                    let nullable_column = column.as_nullable().unwrap();
                    nullable_column.column.as_map().unwrap()
                } else {
                    column.as_map().unwrap()
                };
                let kv_column = KvPair::<AnyType, AnyType>::try_downcast_column(
                    &map_column.underlying_column(),
                )
                .unwrap();
                traverse_column_recursive(
                    &kv_column.keys,
                    &inner_types[0],
                    next_column_id,
                    leaves,
                )?;
                traverse_column_recursive(
                    &kv_column.values,
                    &inner_types[1],
                    next_column_id,
                    leaves,
                )?;
            }
            _ => unreachable!(),
        },
        _ => {
            if RangeIndex::supported_type(data_type) {
                leaves.push((
                    *next_column_id,
                    Value::Column(column.clone()),
                    data_type.clone(),
                ));
            }
            *next_column_id += 1;
        }
    }
    Ok(())
}

/// Traverse the columns in DFS order, convert them to a flatten columns array sorted by leaf_index.
/// We must ensure that each leaf node is traversed, otherwise we may get an incorrect leaf_index.
///
/// For the `Array, `Map` and `Tuple` types, we should expand its inner columns.
fn traverse_scalar_recursive(
    scalar: &Scalar,
    data_type: &DataType,
    next_column_id: &mut ColumnId,
    leaves: &mut Vec<(ColumnId, Value<AnyType>, DataType)>,
) -> databend_common_exception::Result<()> {
    match data_type.remove_nullable() {
        DataType::Tuple(inner_types) => {
            let inner_scalars = if data_type.is_nullable() && *scalar == Scalar::Null {
                inner_types.iter().map(Scalar::default_value).collect()
            } else {
                scalar.as_tuple().unwrap().clone()
            };
            for (inner_scalar, inner_type) in inner_scalars.iter().zip(inner_types.iter()) {
                traverse_scalar_recursive(inner_scalar, inner_type, next_column_id, leaves)?;
            }
        }
        DataType::Array(inner_type) => {
            let array_column = if data_type.is_nullable() && *scalar == Scalar::Null {
                ColumnBuilder::with_capacity(&inner_type, 0).build()
            } else {
                scalar.as_array().unwrap().clone()
            };
            traverse_column_recursive(&array_column, &inner_type, next_column_id, leaves)?;
        }
        DataType::Map(inner_type) => match *inner_type {
            DataType::Tuple(ref inner_types) => {
                let map_column = if data_type.is_nullable() && *scalar == Scalar::Null {
                    Column::Tuple(
                        inner_types
                            .iter()
                            .map(|t| ColumnBuilder::with_capacity(t, 0).build())
                            .collect(),
                    )
                } else {
                    scalar.as_map().unwrap().clone()
                };
                traverse_column_recursive(&map_column, &inner_type, next_column_id, leaves)?;
            }
            _ => unreachable!(),
        },
        _ => {
            // Ignore the range index does not supported type.
            if RangeIndex::supported_type(data_type) {
                leaves.push((
                    *next_column_id,
                    Value::Scalar(scalar.clone()),
                    data_type.clone(),
                ));
            }
            *next_column_id += 1;
        }
    }
    Ok(())
}
