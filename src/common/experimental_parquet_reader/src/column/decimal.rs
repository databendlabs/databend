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

//! Decimal column deserialization for Parquet data

use databend_common_column::buffer::Buffer;
use databend_common_expression::types::i256;
use databend_common_expression::types::DecimalColumn;
use databend_common_expression::types::DecimalSize;
use databend_common_expression::Column;
use parquet2::schema::types::PhysicalType;

use crate::column::common::DictionarySupport;
use crate::column::common::ParquetColumnIterator;
use crate::column::common::ParquetColumnType;
use crate::reader::decompressor::Decompressor;

// =============================================================================
// Wrapper Types for Decimal Usage
// =============================================================================

/// Wrapper for i64 as Decimal64 - enables zero-cost transmute via #[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq)]
#[repr(transparent)]
pub struct Decimal64(pub i64);

/// Wrapper for i128 as Decimal128 - enables zero-cost transmute via #[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq)]
#[repr(transparent)]
pub struct Decimal128(pub i128);

/// Wrapper for i256 as Decimal256 - enables zero-cost transmute via #[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq)]
#[repr(transparent)]
pub struct Decimal256(pub i256);

#[derive(Clone)]
pub struct DecimalMetadata {
    pub precision: u8,
    pub scale: u8,
}

impl ParquetColumnType for Decimal64 {
    type Metadata = DecimalMetadata;
    const PHYSICAL_TYPE: PhysicalType = PhysicalType::Int64;

    fn create_column(data: Vec<Self>, metadata: &Self::Metadata) -> Column {
        let decimal_size = DecimalSize::new_unchecked(metadata.precision, metadata.scale);
        let raw_data: Vec<i64> = unsafe { std::mem::transmute(data) };
        Column::Decimal(DecimalColumn::Decimal64(
            Buffer::from(raw_data),
            decimal_size,
        ))
    }
}

impl ParquetColumnType for Decimal128 {
    type Metadata = DecimalMetadata;
    const PHYSICAL_TYPE: PhysicalType = PhysicalType::FixedLenByteArray(16);

    fn create_column(data: Vec<Self>, metadata: &Self::Metadata) -> Column {
        let decimal_size = DecimalSize::new_unchecked(metadata.precision, metadata.scale);
        let raw_data: Vec<i128> = unsafe { std::mem::transmute(data) };
        Column::Decimal(DecimalColumn::Decimal128(
            Buffer::from(raw_data),
            decimal_size,
        ))
    }
}

impl ParquetColumnType for Decimal256 {
    type Metadata = DecimalMetadata;
    const PHYSICAL_TYPE: PhysicalType = PhysicalType::FixedLenByteArray(32);

    fn create_column(data: Vec<Self>, metadata: &Self::Metadata) -> Column {
        let decimal_size = DecimalSize::new_unchecked(metadata.precision, metadata.scale);
        let raw_data: Vec<i256> = unsafe { std::mem::transmute(data) };
        Column::Decimal(DecimalColumn::Decimal256(
            Buffer::from(raw_data),
            decimal_size,
        ))
    }
}

// =============================================================================
// Dictionary Support Implementation
// =============================================================================

impl DictionarySupport for Decimal64 {
    fn from_dictionary_entry(entry: &[u8]) -> databend_common_exception::Result<Self> {
        if entry.len() != 8 {
            return Err(databend_common_exception::ErrorCode::Internal(format!(
                "Invalid Decimal64 dictionary entry length: expected 8, got {}",
                entry.len()
            )));
        }

        // Parquet stores integers in little-endian format
        let bytes: [u8; 8] = entry.try_into().map_err(|_| {
            databend_common_exception::ErrorCode::Internal(
                "Failed to convert bytes to Decimal64".to_string(),
            )
        })?;

        Ok(Decimal64(i64::from_le_bytes(bytes)))
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

impl DictionarySupport for Decimal128 {
    fn from_dictionary_entry(entry: &[u8]) -> databend_common_exception::Result<Self> {
        if entry.len() != 16 {
            return Err(databend_common_exception::ErrorCode::Internal(format!(
                "Invalid Decimal128 dictionary entry length: expected 16, got {}",
                entry.len()
            )));
        }

        // Parquet stores integers in little-endian format
        let bytes: [u8; 16] = entry.try_into().map_err(|_| {
            databend_common_exception::ErrorCode::Internal(
                "Failed to convert bytes to Decimal128".to_string(),
            )
        })?;

        Ok(Decimal128(i128::from_le_bytes(bytes)))
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

impl DictionarySupport for Decimal256 {
    fn from_dictionary_entry(entry: &[u8]) -> databend_common_exception::Result<Self> {
        if entry.len() != 32 {
            return Err(databend_common_exception::ErrorCode::Internal(format!(
                "Invalid Decimal256 dictionary entry length: expected 32, got {}",
                entry.len()
            )));
        }

        // Parquet stores integers in little-endian format
        let bytes: [u8; 32] = entry.try_into().map_err(|_| {
            databend_common_exception::ErrorCode::Internal(
                "Failed to convert bytes to Decimal256".to_string(),
            )
        })?;

        // Create i256 from bytes
        let value = i256::from_le_bytes(bytes);
        Ok(Decimal256(value))
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

// =============================================================================
// Iterator Type Aliases
// =============================================================================

pub type DecimalIter<'a, T> = ParquetColumnIterator<'a, T>;

// =============================================================================
// Constructor Functions
// =============================================================================

/// Generic decimal iterator constructor
pub fn new_decimal_iter<T>(
    pages: Decompressor,
    num_rows: usize,
    precision: u8,
    scale: u8,
    is_nullable: bool,
    chunk_size: Option<usize>,
) -> DecimalIter<T>
where
    T: ParquetColumnType<Metadata = DecimalMetadata> + DictionarySupport,
{
    let metadata = DecimalMetadata { precision, scale };
    ParquetColumnIterator::new(pages, num_rows, is_nullable, metadata, chunk_size)
}

pub fn new_decimal64_iter(
    pages: Decompressor,
    num_rows: usize,
    precision: u8,
    scale: u8,
    is_nullable: bool,
    chunk_size: Option<usize>,
) -> DecimalIter<Decimal64> {
    new_decimal_iter(pages, num_rows, precision, scale, is_nullable, chunk_size)
}

pub fn new_decimal128_iter(
    pages: Decompressor,
    num_rows: usize,
    precision: u8,
    scale: u8,
    is_nullable: bool,
    chunk_size: Option<usize>,
) -> DecimalIter<Decimal128> {
    new_decimal_iter(pages, num_rows, precision, scale, is_nullable, chunk_size)
}

pub fn new_decimal256_iter(
    pages: Decompressor,
    num_rows: usize,
    precision: u8,
    scale: u8,
    is_nullable: bool,
    chunk_size: Option<usize>,
) -> DecimalIter<Decimal256> {
    new_decimal_iter(pages, num_rows, precision, scale, is_nullable, chunk_size)
}

#[cfg(test)]
mod tests {
    use databend_common_exception::Result;

    use super::*;

    #[test]
    fn test_decimal64_dictionary_support() -> Result<()> {
        // Test from_dictionary_entry
        let entry = [42u8, 0, 0, 0, 0, 0, 0, 0]; // 42 in little-endian
        let value = Decimal64::from_dictionary_entry(&entry)?;
        assert_eq!(value.0, 42);

        // Test negative number
        let entry = [255u8, 255, 255, 255, 255, 255, 255, 255]; // -1 in little-endian
        let value = Decimal64::from_dictionary_entry(&entry)?;
        assert_eq!(value.0, -1);

        // Test invalid entry size
        let entry = [42u8, 0, 0, 0, 0, 0, 0]; // Only 7 bytes
        let result = Decimal64::from_dictionary_entry(&entry);
        assert!(result.is_err());

        Ok(())
    }

    #[test]
    fn test_decimal128_dictionary_support() -> Result<()> {
        // Test from_dictionary_entry
        let entry = [42u8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]; // 42 in little-endian
        let value = Decimal128::from_dictionary_entry(&entry)?;
        assert_eq!(value.0, 42);

        // Test large number
        let mut entry = [0u8; 16];
        entry[0] = 255;
        entry[1] = 255;
        entry[2] = 255;
        entry[3] = 255;
        entry[4] = 255;
        entry[5] = 255;
        entry[6] = 255;
        entry[7] = 127; // i128::MAX lower 64 bits
        let value = Decimal128::from_dictionary_entry(&entry)?;
        assert_eq!(value.0, 9223372036854775807i128);

        // Test invalid entry size
        let entry = [42u8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]; // Only 15 bytes
        let result = Decimal128::from_dictionary_entry(&entry);
        assert!(result.is_err());

        Ok(())
    }

    #[test]
    fn test_decimal256_dictionary_support() -> Result<()> {
        // Test from_dictionary_entry
        let mut entry = [0u8; 32];
        entry[0] = 42; // 42 in little-endian
        let value = Decimal256::from_dictionary_entry(&entry)?;
        let expected = i256::from_le_bytes(entry);
        assert_eq!(value.0, expected);

        // Test all bytes set
        let entry = [255u8; 32];
        let value = Decimal256::from_dictionary_entry(&entry)?;
        let expected = i256::from_le_bytes(entry);
        assert_eq!(value.0, expected);

        // Test invalid entry size
        let entry = [42u8; 31]; // Only 31 bytes
        let result = Decimal256::from_dictionary_entry(&entry);
        assert!(result.is_err());

        Ok(())
    }

    #[test]
    fn test_decimal64_batch_from_dictionary_into_slice() -> Result<()> {
        // Setup dictionary
        let dictionary = vec![
            Decimal64(100),
            Decimal64(200),
            Decimal64(300),
            Decimal64(400),
            Decimal64(500),
        ];

        // Test normal indices
        let indices = [0i32, 2, 4, 1, 3];
        let mut output = vec![Decimal64(0); 5];

        Decimal64::batch_from_dictionary_into_slice(&dictionary, &indices, &mut output)?;
        assert_eq!(output, vec![
            Decimal64(100),
            Decimal64(300),
            Decimal64(500),
            Decimal64(200),
            Decimal64(400)
        ]);

        // Test repeated indices
        let indices = [1i32, 1, 1, 1];
        let mut output = vec![Decimal64(0); 4];

        Decimal64::batch_from_dictionary_into_slice(&dictionary, &indices, &mut output)?;
        assert_eq!(output, vec![
            Decimal64(200),
            Decimal64(200),
            Decimal64(200),
            Decimal64(200)
        ]);

        Ok(())
    }

    #[test]
    fn test_decimal128_batch_from_dictionary_into_slice() -> Result<()> {
        // Setup dictionary
        let dictionary = vec![Decimal128(1000), Decimal128(2000), Decimal128(3000)];

        // Test normal indices
        let indices = [2i32, 0, 1, 2, 0];
        let mut output = vec![Decimal128(0); 5];

        Decimal128::batch_from_dictionary_into_slice(&dictionary, &indices, &mut output)?;
        assert_eq!(output, vec![
            Decimal128(3000),
            Decimal128(1000),
            Decimal128(2000),
            Decimal128(3000),
            Decimal128(1000)
        ]);

        Ok(())
    }

    #[test]
    fn test_decimal256_batch_from_dictionary_into_slice() -> Result<()> {
        // Setup dictionary
        let dictionary = vec![
            Decimal256(i256::from(10)),
            Decimal256(i256::from(20)),
            Decimal256(i256::from(30)),
        ];

        // Test normal indices
        let indices = [1i32, 0, 2];
        let mut output = vec![Decimal256(i256::from(0)); 3];

        Decimal256::batch_from_dictionary_into_slice(&dictionary, &indices, &mut output)?;
        assert_eq!(output, vec![
            Decimal256(i256::from(20)),
            Decimal256(i256::from(10)),
            Decimal256(i256::from(30))
        ]);

        Ok(())
    }

    #[test]
    fn test_decimal_bounds_checking() -> Result<()> {
        let dictionary = vec![Decimal64(10), Decimal64(20), Decimal64(30)];

        // Test out of bounds index
        let indices = [0i32, 3, 1]; // Index 3 is out of bounds
        let mut output = vec![Decimal64(0); 3];

        let result =
            Decimal64::batch_from_dictionary_into_slice(&dictionary, &indices, &mut output);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Dictionary index out of bounds"));

        // Test negative index
        let indices = [0i32, -1, 1];
        let mut output = vec![Decimal64(0); 3];

        let result =
            Decimal64::batch_from_dictionary_into_slice(&dictionary, &indices, &mut output);
        assert!(result.is_err());

        Ok(())
    }

    #[test]
    fn test_decimal_empty_cases() -> Result<()> {
        // Test empty indices with non-empty dictionary
        let dictionary = vec![Decimal128(10), Decimal128(20), Decimal128(30)];
        let indices: [i32; 0] = [];
        let mut output: Vec<Decimal128> = vec![];

        Decimal128::batch_from_dictionary_into_slice(&dictionary, &indices, &mut output)?;
        assert_eq!(output.len(), 0);

        // Test empty dictionary with empty indices
        let dictionary: Vec<Decimal128> = vec![];
        let indices: [i32; 0] = [];
        let mut output: Vec<Decimal128> = vec![];

        Decimal128::batch_from_dictionary_into_slice(&dictionary, &indices, &mut output)?;
        assert_eq!(output.len(), 0);

        Ok(())
    }

    #[test]
    fn test_decimal_mismatched_output_slice_length() -> Result<()> {
        let dictionary = vec![Decimal256(i256::from(10)), Decimal256(i256::from(20))];
        let indices = [0i32, 1, 0];
        let mut output = vec![Decimal256(i256::from(0)); 2]; // Output too small

        let result =
            Decimal256::batch_from_dictionary_into_slice(&dictionary, &indices, &mut output);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Output slice length mismatch"));

        Ok(())
    }
}
