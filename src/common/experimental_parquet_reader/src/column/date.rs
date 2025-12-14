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

use databend_common_expression::Column;
use parquet2::schema::types::PhysicalType;

use crate::column::common::DictionarySupport;
use crate::column::common::ParquetColumnIterator;
use crate::column::common::ParquetColumnType;
use crate::column::number::IntegerMetadata;

/// Date type alias for i32 (days since epoch)
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Date(i32);

impl ParquetColumnType for Date {
    type Metadata = IntegerMetadata;
    const PHYSICAL_TYPE: PhysicalType = PhysicalType::Int32;

    fn create_column(data: Vec<Self>, _metadata: &Self::Metadata) -> Column {
        let raw_data: Vec<i32> = unsafe { std::mem::transmute(data) };
        Column::Date(raw_data.into())
    }
}

// =============================================================================
// Dictionary Support Implementation
// =============================================================================

impl DictionarySupport for Date {
    fn from_dictionary_entry(entry: &[u8]) -> databend_common_exception::Result<Self> {
        if entry.len() != 4 {
            return Err(databend_common_exception::ErrorCode::Internal(format!(
                "Invalid Date dictionary entry length: expected 4, got {}",
                entry.len()
            )));
        }

        // Parquet stores dates as i32 in little-endian format
        let bytes: [u8; 4] = entry.try_into().map_err(|_| {
            databend_common_exception::ErrorCode::Internal(
                "Failed to convert bytes to Date".to_string(),
            )
        })?;

        Ok(Date(i32::from_le_bytes(bytes)))
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

pub type DateIter<'a> = ParquetColumnIterator<'a, Date>;

#[cfg(test)]
mod tests {
    use databend_common_exception::Result;

    use super::*;

    #[test]
    fn test_date_dictionary_support() -> Result<()> {
        // Test from_dictionary_entry
        let entry = [42u8, 0, 0, 0]; // 42 in little-endian (days since epoch)
        let value = Date::from_dictionary_entry(&entry)?;
        assert_eq!(value, Date(42));

        // Test zero date (epoch)
        let entry = [0u8, 0, 0, 0]; // 0 in little-endian
        let value = Date::from_dictionary_entry(&entry)?;
        assert_eq!(value, Date(0));

        // Test large date value
        let entry = [255u8, 255, 255, 127]; // i32::MAX in little-endian
        let value = Date::from_dictionary_entry(&entry)?;
        assert_eq!(value, Date(i32::MAX));

        // Test invalid entry size
        let entry = [42u8, 0, 0]; // Only 3 bytes
        let result = Date::from_dictionary_entry(&entry);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Expected 4 bytes"));

        Ok(())
    }

    #[test]
    fn test_date_batch_from_dictionary_into_slice() -> Result<()> {
        // Setup dictionary with various dates
        let dictionary = vec![
            Date(0),     // 1970-01-01 (epoch)
            Date(365),   // 1971-01-01 (1 year later)
            Date(730),   // 1972-01-01 (2 years later)
            Date(1095),  // 1973-01-01 (3 years later)
            Date(18262), // 2020-01-01 (50 years later)
        ];

        // Test normal indices
        let indices = [0i32, 2, 4, 1, 3];
        let mut output = vec![Date(0); 5];

        Date::batch_from_dictionary_into_slice(&dictionary, &indices, &mut output)?;
        assert_eq!(output, vec![
            Date(0),
            Date(730),
            Date(18262),
            Date(365),
            Date(1095)
        ]);

        // Test repeated indices
        let indices = [4i32, 4, 4, 4]; // All point to 2020-01-01
        let mut output = vec![Date(0); 4];

        Date::batch_from_dictionary_into_slice(&dictionary, &indices, &mut output)?;
        assert_eq!(output, vec![
            Date(18262),
            Date(18262),
            Date(18262),
            Date(18262)
        ]);

        Ok(())
    }

    #[test]
    fn test_date_bounds_checking() -> Result<()> {
        let dictionary = vec![Date(0), Date(365), Date(730)]; // 3 dates

        // Test out of bounds index
        let indices = [0i32, 3, 1]; // Index 3 is out of bounds
        let mut output = vec![Date(0); 3];

        let result = Date::batch_from_dictionary_into_slice(&dictionary, &indices, &mut output);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Dictionary index out of bounds"));

        // Test negative index (should be caught as out of bounds when cast to usize)
        let indices = [0i32, -1, 1];
        let mut output = vec![Date(0); 3];

        let result = Date::batch_from_dictionary_into_slice(&dictionary, &indices, &mut output);
        assert!(result.is_err());

        Ok(())
    }

    #[test]
    fn test_date_empty_cases() -> Result<()> {
        // Test empty indices with non-empty dictionary
        let dictionary = vec![Date(0), Date(365), Date(730)];
        let indices: [i32; 0] = [];
        let mut output: Vec<Date> = vec![];

        Date::batch_from_dictionary_into_slice(&dictionary, &indices, &mut output)?;
        assert_eq!(output.len(), 0);

        // Test empty dictionary with empty indices
        let dictionary: Vec<Date> = vec![];
        let indices: [i32; 0] = [];
        let mut output: Vec<Date> = vec![];

        Date::batch_from_dictionary_into_slice(&dictionary, &indices, &mut output)?;
        assert_eq!(output.len(), 0);

        Ok(())
    }

    #[test]
    fn test_date_mismatched_output_slice_length() -> Result<()> {
        let dictionary = vec![Date(0), Date(365), Date(730)];
        let indices = [0i32, 1, 2];
        let mut output = vec![Date(0); 2]; // Output too small

        let result = Date::batch_from_dictionary_into_slice(&dictionary, &indices, &mut output);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Output slice length mismatch"));

        Ok(())
    }

    #[test]
    fn test_date_physical_type() {
        assert_eq!(Date::PHYSICAL_TYPE, PhysicalType::Int32);
    }

    #[test]
    fn test_date_edge_cases() -> Result<()> {
        // Test with minimum i32 value (very old date)
        let entry = [0u8, 0, 0, 128]; // i32::MIN in little-endian
        let value = Date::from_dictionary_entry(&entry)?;
        assert_eq!(value, Date(i32::MIN));

        // Test dictionary with single element
        let dictionary = vec![Date(12345)];
        let indices = [0i32];
        let mut output = vec![Date(0); 1];

        Date::batch_from_dictionary_into_slice(&dictionary, &indices, &mut output)?;
        assert_eq!(output, vec![Date(12345)]);

        Ok(())
    }
}
