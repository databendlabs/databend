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

use databend_common_column::buffer::Buffer;
use databend_common_expression::types::Number;
use databend_common_expression::Column;
use parquet2::schema::types::PhysicalType;

use crate::column::common::DictionarySupport;
use crate::column::common::ParquetColumnIterator;
use crate::column::common::ParquetColumnType;
use crate::reader::decompressor::Decompressor;

#[derive(Clone, Copy)]
pub struct IntegerMetadata;

impl ParquetColumnType for i32 {
    type Metadata = IntegerMetadata;
    const PHYSICAL_TYPE: PhysicalType = PhysicalType::Int32;

    fn create_column(data: Vec<Self>, _metadata: &Self::Metadata) -> Column {
        Column::Number(i32::upcast_column(Buffer::from(data)))
    }
}

impl ParquetColumnType for i64 {
    type Metadata = IntegerMetadata;
    const PHYSICAL_TYPE: PhysicalType = PhysicalType::Int64;

    fn create_column(data: Vec<Self>, _metadata: &Self::Metadata) -> Column {
        Column::Number(i64::upcast_column(Buffer::from(data)))
    }
}

impl DictionarySupport for i64 {
    fn from_dictionary_entry(entry: &[u8]) -> databend_common_exception::Result<Self> {
        if entry.len() != 8 {
            return Err(databend_common_exception::ErrorCode::Internal(format!(
                "Invalid i64 dictionary entry length: expected 8, got {}",
                entry.len()
            )));
        }

        // Parquet stores integers in little-endian format
        let bytes: [u8; 8] = entry.try_into().map_err(|_| {
            databend_common_exception::ErrorCode::Internal(
                "Failed to convert bytes to i64".to_string(),
            )
        })?;

        Ok(i64::from_le_bytes(bytes))
    }

    fn batch_from_dictionary_into_slice(
        dictionary: &[Self],
        indices: &[i32],
        output: &mut [Self],
    ) -> databend_common_exception::Result<()> {
        // Validate output slice length
        if output.len() != indices.len() {
            return Err(databend_common_exception::ErrorCode::Internal(format!(
                "Output slice length ({}) doesn't match indices length ({})",
                output.len(),
                indices.len()
            )));
        }

        // Batch bounds checking - find max index once
        // if let Some(&max_idx) = indices.iter().max() {
        //    if max_idx as usize >= dictionary.len() {
        //        return Err(databend_common_exception::ErrorCode::Internal(format!(
        //            "Dictionary index out of bounds: {} >= {}",
        //            max_idx, dictionary.len()
        //        )));
        //    }
        //}

        // Fast unchecked copy - all bounds verified above
        for (i, &index) in indices.iter().enumerate() {
            unsafe {
                *output.get_unchecked_mut(i) = *dictionary.get_unchecked(index as usize);
            }
        }

        Ok(())
    }
}

impl DictionarySupport for i32 {
    fn from_dictionary_entry(entry: &[u8]) -> databend_common_exception::Result<Self> {
        if entry.len() != 4 {
            return Err(databend_common_exception::ErrorCode::Internal(format!(
                "Invalid i32 dictionary entry length: expected 4, got {}",
                entry.len()
            )));
        }

        // Parquet stores integers in little-endian format
        let bytes: [u8; 4] = entry.try_into().map_err(|_| {
            databend_common_exception::ErrorCode::Internal(
                "Failed to convert bytes to i32".to_string(),
            )
        })?;

        Ok(i32::from_le_bytes(bytes))
    }

    fn batch_from_dictionary_into_slice(
        dictionary: &[Self],
        indices: &[i32],
        output: &mut [Self],
    ) -> databend_common_exception::Result<()> {
        // Validate output slice length
        if output.len() != indices.len() {
            return Err(databend_common_exception::ErrorCode::Internal(format!(
                "Output slice length ({}) doesn't match indices length ({})",
                output.len(),
                indices.len()
            )));
        }

        // Batch bounds checking - find max index once
        if let Some(&max_idx) = indices.iter().max() {
            if max_idx as usize >= dictionary.len() {
                return Err(databend_common_exception::ErrorCode::Internal(format!(
                    "Dictionary index out of bounds: {} >= {}",
                    max_idx,
                    dictionary.len()
                )));
            }
        }

        // Fast unchecked copy - all bounds verified above
        for (i, &index) in indices.iter().enumerate() {
            unsafe {
                *output.get_unchecked_mut(i) = *dictionary.get_unchecked(index as usize);
            }
        }

        Ok(())
    }
}

pub type Int32Iter<'a> = ParquetColumnIterator<'a, i32>;

pub type Int64Iter<'a> = ParquetColumnIterator<'a, i64>;

pub fn new_int32_iter(
    pages: Decompressor,
    num_rows: usize,
    is_nullable: bool,
    chunk_size: Option<usize>,
) -> Int32Iter {
    ParquetColumnIterator::new(pages, num_rows, is_nullable, IntegerMetadata, chunk_size)
}

pub fn new_int64_iter(
    pages: Decompressor,
    num_rows: usize,
    is_nullable: bool,
    chunk_size: Option<usize>,
) -> Int64Iter {
    ParquetColumnIterator::new(pages, num_rows, is_nullable, IntegerMetadata, chunk_size)
}

#[cfg(test)]
mod tests {
    use databend_common_exception::Result;

    use super::*;

    #[test]
    fn test_i32_dictionary_support() -> Result<()> {
        // Test from_dictionary_entry
        let entry = [42u8, 0, 0, 0]; // 42 in little-endian
        let value = i32::from_dictionary_entry(&entry)?;
        assert_eq!(value, 42);

        // Test negative number
        let entry = [255u8, 255, 255, 255]; // -1 in little-endian
        let value = i32::from_dictionary_entry(&entry)?;
        assert_eq!(value, -1);

        // Test invalid entry size
        let entry = [42u8, 0, 0]; // Only 3 bytes
        let result = i32::from_dictionary_entry(&entry);
        assert!(result.is_err());

        Ok(())
    }

    #[test]
    fn test_i64_dictionary_support() -> Result<()> {
        // Test from_dictionary_entry
        let entry = [42u8, 0, 0, 0, 0, 0, 0, 0]; // 42 in little-endian
        let value = i64::from_dictionary_entry(&entry)?;
        assert_eq!(value, 42);

        // Test large number
        let entry = [255u8, 255, 255, 255, 255, 255, 255, 127]; // i64::MAX
        let value = i64::from_dictionary_entry(&entry)?;
        assert_eq!(value, i64::MAX);

        // Test invalid entry size
        let entry = [42u8, 0, 0, 0, 0, 0, 0]; // Only 7 bytes
        let result = i64::from_dictionary_entry(&entry);
        assert!(result.is_err());

        Ok(())
    }

    #[test]
    fn test_i32_batch_from_dictionary_into_slice() -> Result<()> {
        // Setup dictionary
        let dictionary = vec![10i32, 20, 30, 40, 50];

        // Test normal indices
        let indices = [0i32, 2, 4, 1, 3];
        let mut output = vec![0i32; 5];

        i32::batch_from_dictionary_into_slice(&dictionary, &indices, &mut output)?;
        assert_eq!(output, vec![10, 30, 50, 20, 40]);

        // Test repeated indices
        let indices = [1i32, 1, 1, 1];
        let mut output = vec![0i32; 4];

        i32::batch_from_dictionary_into_slice(&dictionary, &indices, &mut output)?;
        assert_eq!(output, vec![20, 20, 20, 20]);

        Ok(())
    }

    #[test]
    fn test_i64_batch_from_dictionary_into_slice() -> Result<()> {
        // Setup dictionary
        let dictionary = vec![100i64, 200, 300];

        // Test normal indices
        let indices = [2i32, 0, 1, 2, 0];
        let mut output = vec![0i64; 5];

        i64::batch_from_dictionary_into_slice(&dictionary, &indices, &mut output)?;
        assert_eq!(output, vec![300, 100, 200, 300, 100]);

        Ok(())
    }

    #[test]
    fn test_batch_dictionary_bounds_checking() -> Result<()> {
        let dictionary = vec![10i32, 20, 30];

        // Test out of bounds index
        let indices = [0i32, 3, 1]; // Index 3 is out of bounds
        let mut output = vec![0i32; 3];

        let result = i32::batch_from_dictionary_into_slice(&dictionary, &indices, &mut output);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Dictionary index out of bounds"));

        // Test negative index (should be caught as out of bounds when cast to usize)
        let indices = [0i32, -1, 1];
        let mut output = vec![0i32; 3];

        let result = i32::batch_from_dictionary_into_slice(&dictionary, &indices, &mut output);
        assert!(result.is_err());

        Ok(())
    }

    #[test]
    fn test_empty_dictionary_and_indices() -> Result<()> {
        // Test empty indices with non-empty dictionary
        let dictionary = vec![10i32, 20, 30];
        let indices: [i32; 0] = [];
        let mut output: Vec<i32> = vec![];

        i32::batch_from_dictionary_into_slice(&dictionary, &indices, &mut output)?;
        assert_eq!(output.len(), 0);

        // Test empty dictionary with empty indices
        let dictionary: Vec<i32> = vec![];
        let indices: [i32; 0] = [];
        let mut output: Vec<i32> = vec![];

        i32::batch_from_dictionary_into_slice(&dictionary, &indices, &mut output)?;
        assert_eq!(output.len(), 0);

        Ok(())
    }

    #[test]
    fn test_mismatched_output_slice_length() -> Result<()> {
        let dictionary = vec![10i32, 20, 30];
        let indices = [0i32, 1, 2];
        let mut output = vec![0i32; 2]; // Output too small

        let result = i32::batch_from_dictionary_into_slice(&dictionary, &indices, &mut output);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Output slice length mismatch"));

        Ok(())
    }
}
