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

use ahash::HashSet;
use ahash::HashSetExt;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::AnyType;
use common_expression::Column;
use common_expression::DataBlock;
use common_expression::Scalar;
use common_expression::ScalarRef;
use common_expression::Value;
use common_functions::aggregates::eval_aggr;

use crate::operations::replace_into::meta::merge_into_operation_meta::DeletionByColumn;
use crate::operations::replace_into::meta::merge_into_operation_meta::MergeIntoOperation;
use crate::operations::replace_into::meta::merge_into_operation_meta::UniqueKeyDigest;
use crate::operations::replace_into::mutator::column_hash::row_hash_of_columns;
use crate::operations::replace_into::OnConflictField;

// Replace is somehow a simplified merge_into, which
// - do insertion for "matched" branch
// - update for "not-matched" branch (by sending MergeIntoOperation to downstream)
pub struct ReplaceIntoMutator {
    on_conflict_fields: Vec<OnConflictField>,
    key_saw: HashSet<UniqueKeyDigest>,
}

impl ReplaceIntoMutator {
    pub fn create(on_conflict_fields: Vec<OnConflictField>) -> Self {
        Self {
            on_conflict_fields,
            key_saw: Default::default(),
        }
    }
}

enum ColumnHash {
    NoConflict(HashSet<UniqueKeyDigest>),
    // the first row index that has conflict
    Conflict(usize),
}

impl ReplaceIntoMutator {
    pub fn process_input_block(&mut self, data_block: &DataBlock) -> Result<MergeIntoOperation> {
        // TODO table level pruning:
        // if we can deduced that `data_block` is insert only, return an MergeIntoOperation::None (None op for Matched Branch)
        self.extract_on_conflict_columns(data_block)
    }

    fn extract_on_conflict_columns(
        &mut self,
        data_block: &DataBlock,
    ) -> Result<MergeIntoOperation> {
        let num_rows = data_block.num_rows();
        let mut column_values = Vec::with_capacity(self.on_conflict_fields.len());
        for field in &self.on_conflict_fields {
            let filed_index = field.field_index;
            let entry = &data_block.columns()[filed_index];
            column_values.push(&entry.value);
        }

        match Self::build_column_hash(&column_values, &mut self.key_saw, num_rows)? {
            ColumnHash::NoConflict(key_hashes) => {
                let columns_min_max = Self::columns_min_max(&column_values, num_rows)?;
                let delete_action = DeletionByColumn {
                    columns_min_max,
                    key_hashes,
                };
                Ok(MergeIntoOperation::Delete(delete_action))
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
            let hash = row_hash_of_columns(column_values, row_idx);
            if saw.contains(&hash) {
                return Ok(ColumnHash::Conflict(row_idx));
            }
            saw.insert(hash);
            digests.insert(hash);
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
