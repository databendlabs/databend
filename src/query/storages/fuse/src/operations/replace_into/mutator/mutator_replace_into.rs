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

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::iter::once;

use ahash::HashSet;
use ahash::HashSetExt;
use common_arrow::arrow::bitmap::MutableBitmap;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::AnyType;
use common_expression::types::DataType;
use common_expression::Column;
use common_expression::ColumnId;
use common_expression::DataBlock;
use common_expression::Evaluator;
use common_expression::Expr;
use common_expression::FieldIndex;
use common_expression::FunctionContext;
use common_expression::RemoteExpr;
use common_expression::Scalar;
use common_expression::ScalarRef;
use common_expression::TableSchema;
use common_expression::Value;
use common_functions::aggregates::eval_aggr;
use common_functions::BUILTIN_FUNCTIONS;
use common_sql::executor::OnConflictField;
use log::info;
use storages_common_index::BloomIndex;
use storages_common_table_meta::meta::ColumnStatistics;
use storages_common_table_meta::meta::MinMax;

use crate::metrics::metrics_inc_replace_original_row_number;
use crate::metrics::metrics_inc_replace_partition_number;
use crate::metrics::metrics_inc_replace_row_number_after_table_level_pruning;
use crate::operations::replace_into::meta::merge_into_operation_meta::DeletionByColumn;
use crate::operations::replace_into::meta::merge_into_operation_meta::MergeIntoOperation;
use crate::operations::replace_into::meta::merge_into_operation_meta::UniqueKeyDigest;
use crate::operations::replace_into::mutator::column_hash::row_hash_of_columns;
use crate::operations::replace_into::mutator::column_hash::RowScalarValue;

// Replace is somehow a simplified merge_into, which
// - do insertion for "matched" branch
// - update for "not-matched" branch (by sending MergeIntoOperation to downstream)
pub struct ReplaceIntoMutator {
    on_conflict_fields: Vec<OnConflictField>,
    table_range_index: HashMap<ColumnId, ColumnStatistics>,
    key_saw: HashSet<UniqueKeyDigest>,
    partitioner: Option<Partitioner>,
}

impl ReplaceIntoMutator {
    pub fn try_create(
        ctx: &dyn TableContext,
        on_conflict_fields: Vec<OnConflictField>,
        cluster_keys: Vec<RemoteExpr<String>>,
        most_significant_on_conflict_field_index: Option<FieldIndex>,
        table_schema: &TableSchema,
        table_range_idx: HashMap<ColumnId, ColumnStatistics>,
    ) -> Result<Self> {
        let partitioner = if !cluster_keys.is_empty()
            && ctx.get_settings().get_enable_replace_into_partitioning()?
        {
            Some(Partitioner::try_new(
                ctx,
                on_conflict_fields.clone(),
                &cluster_keys,
                most_significant_on_conflict_field_index,
                table_schema,
            )?)
        } else {
            None
        };
        Ok(Self {
            on_conflict_fields,
            table_range_index: table_range_idx,
            key_saw: Default::default(),
            partitioner,
        })
    }
}

enum ColumnHash {
    // no conflict, the hash set contains all the unique key digests
    NoConflict(HashSet<UniqueKeyDigest>),
    // the first row index that has conflict
    Conflict(usize),
}

impl ReplaceIntoMutator {
    pub fn process_input_block(&mut self, data_block: &DataBlock) -> Result<MergeIntoOperation> {
        // pruning rows by using table level range index
        // rows that definitely have no conflict will be removed
        metrics_inc_replace_original_row_number(data_block.num_rows() as u64);
        let data_block_may_have_conflicts = self.table_level_row_prune(data_block)?;

        let row_number_after_pruning = data_block_may_have_conflicts.num_rows();
        metrics_inc_replace_row_number_after_table_level_pruning(row_number_after_pruning as u64);

        if row_number_after_pruning == 0 {
            info!("(replace-into) all rows are append-only");
            return Ok(MergeIntoOperation::None);
        }

        let merge_into_operation = if let Some(partitioner) = &self.partitioner {
            // if table has cluster keys; we partition the input data block by left most column of cluster keys
            let partitions = partitioner.partition(data_block)?;
            metrics_inc_replace_partition_number(partitions.len() as u64);
            let vs = partitions
                .into_iter()
                .map(|partition| {
                    let columns_min_max = partition
                        .columns_min_max
                        .into_iter()
                        .map(|min_max| match min_max {
                            MinMax::Point(v) => (v.clone(), v),
                            MinMax::Range(min, max) => (min, max),
                        })
                        .collect::<Vec<_>>();
                    let key_hashes = partition.digests;
                    DeletionByColumn {
                        columns_min_max,
                        key_hashes,
                        bloom_hashes: Some(partition.bloom_hashes),
                    }
                })
                .collect();
            MergeIntoOperation::Delete(vs)
        } else {
            // otherwise, we just build a single delete action
            self.build_merge_into_operation(&data_block_may_have_conflicts)?
        };
        Ok(merge_into_operation)
    }

    // filter out rows that definitely have no conflict, by using table level range index
    fn table_level_row_prune(&self, data_block: &DataBlock) -> Result<DataBlock> {
        let column_stats: &HashMap<ColumnId, ColumnStatistics> = &self.table_range_index;
        let mut bitmap = MutableBitmap::new();
        // for each row, check if it may have conflict
        for row_idx in 0..data_block.num_rows() {
            let mut should_keep = true;
            // for each column, check if it may have conflict
            for field in &self.on_conflict_fields {
                let column: &Value<AnyType> = &data_block.columns()[field.field_index].value;
                let value = column.row_scalar(row_idx)?;
                let stats = column_stats.get(&field.table_field.column_id);
                if let Some(stats) = stats {
                    should_keep = !(value < stats.min().as_ref() || value > stats.max().as_ref());
                    if !should_keep {
                        // if one column outsides the table level range, no need to check other columns
                        break;
                    }
                }
            }
            bitmap.push(should_keep);
        }
        let bitmap = bitmap.into();
        data_block.clone().filter_with_bitmap(&bitmap)
    }

    fn build_merge_into_operation(&mut self, data_block: &DataBlock) -> Result<MergeIntoOperation> {
        let num_rows = data_block.num_rows();
        let column_values = on_conflict_key_column_values(&self.on_conflict_fields, data_block);

        match Self::build_column_hash(&column_values, &mut self.key_saw, num_rows)? {
            ColumnHash::NoConflict(key_hashes) => {
                let columns_min_max = Self::columns_min_max(&column_values, num_rows)?;
                let delete_action = DeletionByColumn {
                    columns_min_max,
                    key_hashes,
                    bloom_hashes: None,
                };
                Ok(MergeIntoOperation::Delete(vec![delete_action]))
            }
            ColumnHash::Conflict(conflict_row_idx) => {
                let conflict_description = {
                    let conflicts = column_values
                        .iter()
                        .zip(self.on_conflict_fields.iter())
                        .map(|(col, field)| {
                            let col_name = &field.table_field.name;
                            // if col.index(conflict_row_idx) is None, an exception will already be thrown in build_column_hash
                            let row_value = col.index(conflict_row_idx).unwrap();
                            let row_value_message =
                                Self::extract_col_value_for_err_message(row_value);
                            format!("\"{}\":{}", col_name, row_value_message)
                        })
                        .collect::<Vec<_>>()
                        .join(", ");

                    format!("at row {}, [{}]", conflict_row_idx, conflicts)
                };
                Err(ErrorCode::StorageOther(format!(
                    "duplicated data detected in the values being replaced into (only the first one will be described): {}",
                    conflict_description
                )))
            }
        }
    }

    fn build_column_hash(
        column_values: &[&Value<AnyType>],
        saw: &mut HashSet<UniqueKeyDigest>,
        num_rows: usize,
    ) -> Result<ColumnHash> {
        let mut digests = HashSet::new();
        for row_idx in 0..num_rows {
            if let Some(hash) = row_hash_of_columns(column_values, row_idx)? {
                if saw.contains(&hash) {
                    return Ok(ColumnHash::Conflict(row_idx));
                }
                saw.insert(hash);
                digests.insert(hash);
            }
        }
        Ok(ColumnHash::NoConflict(digests))
    }

    fn eval(column: Column, num_rows: usize, aggr_func_name: &str) -> Result<Scalar> {
        let (state, _) = eval_aggr(aggr_func_name, vec![], &[column], num_rows)?;
        if state.len() > 0 {
            if let Some(v) = state.index(0) {
                return Ok(v.to_owned());
            }
        }
        Err(ErrorCode::Internal(
            "evaluation min max value of given column failed",
        ))
    }

    fn columns_min_max(
        columns: &[&Value<AnyType>],
        num_rows: usize,
    ) -> Result<Vec<(Scalar, Scalar)>> {
        let mut res = Vec::with_capacity(columns.len());
        for column in columns {
            let (min, max) = match column {
                Value::Scalar(s) => (s.clone(), s.clone()),
                Value::Column(c) => {
                    let min = Self::eval((*c).clone(), num_rows, "min")?;
                    let max = Self::eval((*c).clone(), num_rows, "max")?;
                    (min, max)
                }
            };
            res.push((min, max));
        }
        Ok(res)
    }

    fn extract_col_value_for_err_message(scalar: ScalarRef) -> String {
        // unfortunately, lifetime issues, we can not return a &str
        match scalar {
            // for nested types, just return the type name as a hint.
            ScalarRef::Array(_) => "[ARRAY]".to_owned(),
            ScalarRef::Map(_) => "[MAP]".to_owned(),
            ScalarRef::Bitmap(_) => "[BITMAP]".to_owned(),
            ScalarRef::Tuple(_) => "[TUPLE]".to_owned(),
            ScalarRef::Variant(_) => "[VARIANT]".to_owned(),
            // for string, return the first 5 chars
            ScalarRef::String(s) => {
                let val = String::from_utf8_lossy(s).to_string();
                // take the first 5 chars
                match val.as_str().char_indices().nth(5) {
                    None => val,
                    Some((idx, _)) => format!("{}...", &val[..idx]),
                }
            }
            // for other primitive types, just return the string representation
            v => v.to_string(),
        }
    }
}

#[derive(Debug)]
struct Partition {
    // digests of on-conflict fields, of all the rows in this partition
    digests: HashSet<UniqueKeyDigest>,
    // min max of all the on-conflict fields in this partition
    columns_min_max: Vec<MinMax<Scalar>>,
    // bloom hashes of the most significant on-conflict field
    bloom_hashes: HashSet<u64>,
}

impl Partition {
    fn try_new_with_row(
        row_digest: u128,
        bloom_hash: &Option<&u64>,
        column_values: &[&Value<AnyType>],
        row_idx: usize,
    ) -> Result<Self> {
        let columns_min_max = column_values
            .iter()
            .map(|column| {
                let v = column.row_scalar(row_idx)?.to_owned();
                Ok(MinMax::Point(v))
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Self {
            digests: once(row_digest).collect(),
            columns_min_max,
            bloom_hashes: {
                match bloom_hash {
                    None => HashSet::new(),
                    Some(v) => once(**v).collect(),
                }
            },
        })
    }

    fn push_row(
        &mut self,
        row_digest: u128,
        bloom_hash: &Option<&u64>,
        column_values: &[&Value<AnyType>],
        row_idx: usize,
    ) -> Result<()> {
        self.digests.insert(row_digest);
        if let Some(v) = bloom_hash {
            self.bloom_hashes.insert(**v);
        }
        for (column_idx, min_max) in self.columns_min_max.iter_mut().enumerate() {
            let value: Scalar = column_values[column_idx].row_scalar(row_idx)?.to_owned();
            min_max.update(value);
        }
        Ok(())
    }
}

struct Partitioner {
    on_conflict_fields: Vec<OnConflictField>,
    func_ctx: FunctionContext,
    left_most_cluster_key: Expr,
    // the most significant on-conflict field index chosen, if any
    bloom_filter_column_info: Option<(FieldIndex, DataType)>,
}

impl Partitioner {
    fn try_new(
        ctx: &dyn TableContext,
        on_conflict_fields: Vec<OnConflictField>,
        cluster_keys: &[RemoteExpr<String>],
        most_significant_on_conflict_field_index: Option<FieldIndex>,
        table_schema: &TableSchema,
    ) -> Result<Self> {
        let left_most_cluster_key = &cluster_keys[0];
        let expr: Expr = left_most_cluster_key
            .as_expr(&BUILTIN_FUNCTIONS)
            .project_column_ref(|name| table_schema.index_of(name).unwrap());
        let func_ctx = ctx.get_function_context()?;

        let bloom_filter_column_info = most_significant_on_conflict_field_index.map(|idx| {
            let data_type = (&on_conflict_fields[idx].table_field.data_type).into();
            (idx, data_type)
        });

        Ok(Self {
            on_conflict_fields,
            func_ctx,
            left_most_cluster_key: expr,
            bloom_filter_column_info,
        })
    }

    fn partition(&self, data_block: &DataBlock) -> Result<Vec<Partition>> {
        let evaluator = Evaluator::new(data_block, &self.func_ctx, &BUILTIN_FUNCTIONS);
        let cluster_key_values = evaluator.run(&self.left_most_cluster_key)?;

        // extract the on-conflict column values
        let on_conflict_column_values =
            on_conflict_key_column_values(&self.on_conflict_fields, data_block);

        // partitions by the left-most cluster key expression
        let mut partitions: HashMap<Scalar, Partition> = HashMap::new();

        // bloom hashes of the most significant on-conflict field
        let maybe_bloom_hashes = self
            .bloom_filter_column_info
            .as_ref()
            .and_then(|(idx, typ)| {
                let maybe_col = on_conflict_column_values[*idx].as_column();
                maybe_col.map(|col| {
                    BloomIndex::calculate_nullable_column_digest(&self.func_ctx, col, typ)
                })
            })
            .transpose()?;

        for row_idx in 0..data_block.num_rows() {
            if let Some(row_digest) = row_hash_of_columns(&on_conflict_column_values, row_idx)? {
                let bloom_hash = maybe_bloom_hashes
                    .as_ref()
                    .and_then(|(hashes, validity)| match validity {
                        Some(v) if v.unset_bits() != 0 => v
                            .get(row_idx)
                            .map(|v| if v { hashes.get(row_idx) } else { None }),
                        _ => Some(hashes.get(row_idx)),
                    })
                    .flatten();
                let cluster_key_value = cluster_key_values.row_scalar(row_idx)?.to_owned();

                match partitions.entry(cluster_key_value) {
                    Entry::Occupied(ref mut entry) => entry.get_mut().push_row(
                        row_digest,
                        &bloom_hash,
                        &on_conflict_column_values,
                        row_idx,
                    )?,
                    Entry::Vacant(entry) => {
                        entry.insert(Partition::try_new_with_row(
                            row_digest,
                            &bloom_hash,
                            &on_conflict_column_values,
                            row_idx,
                        )?);
                    }
                }
            }
        }
        Ok(partitions.into_values().collect::<Vec<_>>())
    }
}

fn on_conflict_key_column_values<'a>(
    on_conflict_fields: &[OnConflictField],
    data_block: &'a DataBlock,
) -> Vec<&'a Value<AnyType>> {
    on_conflict_fields
        .iter()
        .map(|field| {
            let filed_index = field.field_index;
            let entry = &data_block.columns()[filed_index];
            &entry.value
        })
        .collect::<Vec<_>>()
}

#[cfg(test)]
mod tests {
    use common_expression::types::NumberType;
    use common_expression::types::StringType;
    use common_expression::FromData;

    use super::*;

    #[test]
    fn test_column_digest() -> Result<()> {
        // ------|---
        // Hi      1
        // hello   2
        let column1 = Value::Column(StringType::from_data(&["Hi", "Hello"]));
        let column2 = Value::Column(NumberType::<u8>::from_data(vec![1, 2]));
        let mut saw = HashSet::new();
        let num_rows = 2;

        let columns = [&column1, &column2];
        let r = ReplaceIntoMutator::build_column_hash(&columns, &mut saw, num_rows)?;
        assert_eq!(saw.len(), 2);
        assert!(matches!(r, ColumnHash::NoConflict(..)));

        // append new item, no conflict
        // ------|---
        // Hi      2
        // hello   3
        let column1 = Value::Column(StringType::from_data(&["Hi", "Hello"]));
        let column2 = Value::Column(NumberType::<u8>::from_data(vec![2, 3]));
        let columns = [&column1, &column2];
        let num_rows = 2;
        let r = ReplaceIntoMutator::build_column_hash(&columns, &mut saw, num_rows)?;
        assert_eq!(saw.len(), 4);
        assert!(matches!(r, ColumnHash::NoConflict(..)));

        // new item, conflict (at row idx 2)
        // ------------|---
        //  not_exist   1
        //  not_exist2  2
        //  Hi          1
        let column1 = Value::Column(StringType::from_data(&["not_exist", "not_exist2", "Hi"]));
        let column2 = Value::Column(NumberType::<u8>::from_data(vec![1, 2, 1]));
        let columns = [&column1, &column2];
        let num_rows = 3;
        let r = ReplaceIntoMutator::build_column_hash(&columns, &mut saw, num_rows)?;
        assert_eq!(saw.len(), 6);
        assert!(matches!(r, ColumnHash::Conflict(2)));

        Ok(())
    }

    #[test]
    fn test_column_min_max() -> Result<()> {
        let column1 = Value::Column(StringType::from_data(vec![
            "a", "b", "c", "d", "c", "b", "a",
        ]));
        let column2 = Value::Column(NumberType::<u8>::from_data(vec![5, 3, 2, 1, 2, 3, 5]));
        let columns = [&column1, &column2];
        let num_rows = 2;
        let min_max_pairs = ReplaceIntoMutator::columns_min_max(&columns, num_rows)?;
        let (min, max) = &min_max_pairs[1];
        assert_eq!(min.to_string(), "1");
        assert_eq!(max.to_string(), "5");
        let (min, max) = &min_max_pairs[0];
        assert_eq!(min.to_string(), "'a'");
        assert_eq!(max.to_string(), "'d'");

        Ok(())
    }
}
