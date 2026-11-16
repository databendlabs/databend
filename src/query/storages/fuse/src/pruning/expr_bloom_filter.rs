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
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::hash_util::hash_by_method_for_bloom;
use databend_common_expression::types::EncodedColumn;
use databend_common_expression::types::MutableBitmap;

pub struct ExprBloomFilter<'a> {
    filter: &'a Sbbf,
}

impl<'a> ExprBloomFilter<'a> {
    pub fn new(filter: &'a Sbbf) -> Self {
        Self { filter }
    }

    pub fn apply(&self, column: Column) -> Result<MutableBitmap> {
        let hashes = bloom_hashes(column.remove_nullable())?;
        let mut bitmap = MutableBitmap::from_len_zeroed(hashes.len());
        self.filter
            .check_hash_batch(&hashes, |index| bitmap.set(index, true));
        Ok(bitmap)
    }
}

/// Rows of a frame of reference column decoded at a time, small enough to stay in cache.
const ENCODED_HASH_CHUNK: usize = 4096;

/// Bloom hash of each row. Encoded columns get the hashes of their decoded values without
/// decoding the whole column: a dictionary hashes each of its values once, a frame of
/// reference column is decoded and hashed in small chunks.
fn bloom_hashes(column: Column) -> Result<Vec<u64>> {
    let encoded = match column {
        Column::Encoded(encoded) => encoded,
        column => return bloom_hashes_decoded(column),
    };
    match encoded.as_ref() {
        EncodedColumn::Dict(dict) => {
            let value_hashes = bloom_hashes_decoded(dict.values.clone())?;
            Ok(dict
                .keys
                .iter()
                .map(|key| value_hashes[key as usize])
                .collect())
        }
        EncodedColumn::For(column) => {
            let mut hashes = Vec::with_capacity(column.len());
            for start in (0..column.len()).step_by(ENCODED_HASH_CHUNK) {
                let end = (start + ENCODED_HASH_CHUNK).min(column.len());
                hashes.extend(bloom_hashes_decoded(column.slice(start..end).decode())?);
            }
            Ok(hashes)
        }
    }
}

fn bloom_hashes_decoded(column: Column) -> Result<Vec<u64>> {
    let num_rows = column.len();
    let method = DataBlock::choose_hash_method_with_types(&[column.data_type()])?;
    let entries = &[column.into()];
    let mut hashes = Vec::with_capacity(num_rows);
    hash_by_method_for_bloom(&method, entries.into(), num_rows, &mut hashes)?;
    Ok(hashes)
}

#[cfg(test)]
mod tests {
    use databend_common_expression::FromData;
    use databend_common_expression::types::DictColumn;
    use databend_common_expression::types::ForColumn;
    use databend_common_expression::types::Int64Type;
    use databend_common_expression::types::StringType;

    use super::*;

    fn assert_same_hashes(column: Column, encoded: EncodedColumn) {
        let expected = bloom_hashes(column).unwrap();
        let actual = bloom_hashes(Column::Encoded(Box::new(encoded))).unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_bloom_hashes_of_encoded_columns() {
        // Several chunks, the last one partial.
        let rows = ENCODED_HASH_CHUNK * 2 + 17;
        let ints = Int64Type::from_data((0..rows as i64).map(|i| 1_000_000 + i * 7).collect());
        let encoded = ForColumn::try_encode(&ints).unwrap();
        assert_same_hashes(ints, EncodedColumn::For(encoded));

        let strings = StringType::from_data(
            (0..rows)
                .map(|i| format!("value-{}", i % 13))
                .collect::<Vec<_>>(),
        );
        let encoded = DictColumn::try_encode(&strings, 64).unwrap();
        assert_same_hashes(strings, EncodedColumn::Dict(encoded));
    }
}
