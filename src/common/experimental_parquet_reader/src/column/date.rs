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

use crate::column::common::ParquetColumnIterator;
use crate::column::common::ParquetColumnType;
use crate::column::common::DictionarySupport;
use crate::column::number::IntegerMetadata;

#[derive(Copy, Clone)]
#[repr(transparent)]
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
            return Err(databend_common_exception::ErrorCode::Internal(
                format!("Invalid Date dictionary entry length: expected 4, got {}", entry.len())
            ));
        }
        
        // Parquet stores dates as i32 in little-endian format
        let bytes: [u8; 4] = entry.try_into().map_err(|_| {
            databend_common_exception::ErrorCode::Internal("Failed to convert bytes to Date".to_string())
        })?;
        
        Ok(Date(i32::from_le_bytes(bytes)))
    }
    
    fn batch_from_dictionary(dictionary: &[Self], indices: &[i32]) -> databend_common_exception::Result<Vec<Self>> {
        // Pre-allocate result vector with exact capacity
        let mut result = Vec::with_capacity(indices.len());
        
        // Use unsafe set_len for maximum performance, then fill in-place
        unsafe {
            result.set_len(indices.len());
        }
        
        // Batch copy with bounds checking
        for (i, &index) in indices.iter().enumerate() {
            let dict_idx = index as usize;
            if dict_idx >= dictionary.len() {
                return Err(databend_common_exception::ErrorCode::Internal(format!(
                    "Dictionary index out of bounds: {} >= {}", 
                    dict_idx, dictionary.len()
                )));
            }
            // Direct assignment to pre-allocated memory
            result[i] = dictionary[dict_idx];
        }
        
        Ok(result)
    }
}

pub type DateIter<'a> = ParquetColumnIterator<'a, Date>;
