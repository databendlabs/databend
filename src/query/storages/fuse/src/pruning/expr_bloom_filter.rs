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

use databend_common_catalog::sbbf::Sbbf;
use databend_common_exception::Result;
use databend_common_expression::hash_util::hash_by_method_for_bloom;
use databend_common_expression::types::MutableBitmap;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;

pub struct ExprBloomFilter<'a> {
    filter: &'a Sbbf,
}

impl<'a> ExprBloomFilter<'a> {
    pub fn new(filter: &'a Sbbf) -> Self {
        Self { filter }
    }

    /// Apply the bloom filter to a column, updating the bitmap.
    pub fn apply(&self, column: Column, bitmap: &mut MutableBitmap) -> Result<()> {
        let data_type = column.data_type();
        let num_rows = column.len();
        let method = DataBlock::choose_hash_method_with_types(&[data_type.clone()])?;
        let entries = &[column.into()];
        let group_columns = entries.into();
        let mut hashes = Vec::with_capacity(num_rows);
        hash_by_method_for_bloom(&method, group_columns, num_rows, &mut hashes)?;
        debug_assert_eq!(hashes.len(), num_rows);
        let bitmap_len = bitmap.len();
        self.filter.check_hash_batch(&hashes, |index| {
            debug_assert!(index < bitmap_len);
            unsafe { bitmap.set_unchecked(index, true) };
        });

        Ok(())
    }
}
