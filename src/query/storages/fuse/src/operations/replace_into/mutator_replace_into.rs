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

use std::collections::HashSet;
use std::hash::Hasher;

use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::Column;
use common_expression::DataBlock;
use common_expression::Scalar;
use common_functions::aggregates::eval_aggr;
use siphasher::sip128;
use siphasher::sip128::Hasher128;

use crate::operations::merge_into::mutation_meta::merge_into_operation_meta::DeletionByColumn;
use crate::operations::merge_into::mutation_meta::merge_into_operation_meta::MergeIntoOperation;
use crate::operations::merge_into::mutation_meta::merge_into_operation_meta::UniqueKeyDigest;
use crate::operations::merge_into::OnConflictField;

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
    Conflict,
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
        let mut columns = Vec::with_capacity(self.on_conflict_fields.len());
        for field in &self.on_conflict_fields {
            let filed_index = field.field_index;
            let entry = &data_block.columns()[filed_index];
            let column = entry.value.as_column().unwrap();
            columns.push(column);
        }
        match Self::build_column_hash(&columns, &mut self.key_saw, num_rows)? {
            ColumnHash::NoConflict(key_hashes) => {
                let columns_min_max = Self::columns_min_max(&columns, num_rows)?;
                let delete_action = DeletionByColumn {
                    columns_min_max,
                    key_hashes,
                };
                Ok(MergeIntoOperation::Delete(delete_action))
            }
            ColumnHash::Conflict => Err(ErrorCode::StorageOther(
                "duplicated data detected in merge source",
            )),
        }
    }

    fn build_column_hash(
        columns: &[&Column],
        saw: &mut HashSet<UniqueKeyDigest>,
        num_rows: usize,
    ) -> Result<ColumnHash> {
        let mut digests = HashSet::new();
        for i in 0..num_rows {
            let mut sip = sip128::SipHasher24::new();
            for column in columns {
                let value = column.index(i).unwrap();
                let string = value.to_string();
                sip.write(string.as_bytes());
            }
            let hash = sip.finish128().as_u128();
            if saw.contains(&hash) {
                return Ok(ColumnHash::Conflict);
            } else {
                saw.insert(hash);
            }
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

    fn columns_min_max(columns: &[&Column], num_rows: usize) -> Result<Vec<(Scalar, Scalar)>> {
        let mut res = Vec::with_capacity(columns.len());
        for column in columns {
            let min = Self::eval((*column).clone(), num_rows, "min")?;
            let max = Self::eval((*column).clone(), num_rows, "max")?;
            res.push((min, max));
        }
        Ok(res)
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
        let column1 = StringType::from_data(&["Hi", "Hello"]);
        let column2 = NumberType::<u8>::from_data(vec![1, 2]);
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
        let column1 = StringType::from_data(&["Hi", "Hello"]);
        let column2 = NumberType::<u8>::from_data(vec![2, 3]);
        let columns = [&column1, &column2];
        let num_rows = 2;
        let r = ReplaceIntoMutator::build_column_hash(&columns, &mut saw, num_rows)?;
        assert_eq!(saw.len(), 4);
        assert!(matches!(r, ColumnHash::NoConflict(..)));

        // new item, conflict
        // ------|---
        //  Hi     1
        let column1 = StringType::from_data(&["Hi"]);
        let column2 = NumberType::<u8>::from_data(vec![1]);
        let columns = [&column1, &column2];
        let num_rows = 1;
        let r = ReplaceIntoMutator::build_column_hash(&columns, &mut saw, num_rows)?;
        assert_eq!(saw.len(), 4);
        assert!(matches!(r, ColumnHash::Conflict));

        Ok(())
    }

    #[test]
    fn test_column_min_max() -> Result<()> {
        let column1 = StringType::from_data(vec!["a", "b", "c", "d", "c", "b", "a"]);
        let column2 = NumberType::<u8>::from_data(vec![5, 3, 2, 1, 2, 3, 5]);
        let columns = [&column1, &column2];
        let num_rows = 2;
        let min_max_pairs = ReplaceIntoMutator::columns_min_max(&columns, num_rows)?;
        let (min, max) = &min_max_pairs[0];
        assert_eq!(min.to_string(), "\"a\"");
        assert_eq!(max.to_string(), "\"d\"");
        let (min, max) = &min_max_pairs[1];
        assert_eq!(min.to_string(), "1");
        assert_eq!(max.to_string(), "5");

        Ok(())
    }
}
