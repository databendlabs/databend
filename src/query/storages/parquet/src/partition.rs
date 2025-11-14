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

use databend_common_expression::Scalar;
use parquet::arrow::arrow_reader::RowSelector;
use parquet::file::metadata::RowGroupMetaData;
use parquet::format::PageLocation;

use crate::row_group_serde::deserialize_row_group_meta;
use crate::row_group_serde::serialize_row_group_meta;

/// Serializable row selector.
#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq, Clone, Debug)]
pub struct SerdeRowSelector {
    pub row_count: usize,
    pub skip: bool,
}

impl From<&RowSelector> for SerdeRowSelector {
    fn from(value: &RowSelector) -> Self {
        SerdeRowSelector {
            row_count: value.row_count,
            skip: value.skip,
        }
    }
}

impl From<&SerdeRowSelector> for RowSelector {
    fn from(value: &SerdeRowSelector) -> Self {
        RowSelector {
            row_count: value.row_count,
            skip: value.skip,
        }
    }
}

/// Serializable page location.
#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq, Clone, Debug)]
pub struct SerdePageLocation {
    pub offset: i64,
    pub compressed_page_size: i32,
    pub first_row_index: i64,
}

impl From<&PageLocation> for SerdePageLocation {
    fn from(value: &PageLocation) -> Self {
        SerdePageLocation {
            offset: value.offset,
            compressed_page_size: value.compressed_page_size,
            first_row_index: value.first_row_index,
        }
    }
}

impl From<&SerdePageLocation> for PageLocation {
    fn from(value: &SerdePageLocation) -> Self {
        PageLocation {
            offset: value.offset,
            compressed_page_size: value.compressed_page_size,
            first_row_index: value.first_row_index,
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Clone, Debug)]
pub struct ParquetRowGroupPart {
    pub location: String,
    pub start_row: u64,
    #[serde(
        serialize_with = "serialize_row_group_meta",
        deserialize_with = "deserialize_row_group_meta"
    )]
    pub meta: RowGroupMetaData,
    pub selectors: Option<Vec<SerdeRowSelector>>,
    pub page_locations: Option<Vec<Vec<SerdePageLocation>>>,
    // `uncompressed_size` and `compressed_size` are the sizes of the actually read columns.
    pub uncompressed_size: u64,
    pub compressed_size: u64,
    pub sort_min_max: Option<(Scalar, Scalar)>,
    // `omit_filter` = true means that the row group is filtered out by the filter.
    pub omit_filter: bool,

    pub schema_index: usize,
}

impl ParquetRowGroupPart {
    pub fn uncompressed_size(&self) -> u64 {
        self.uncompressed_size
    }

    pub fn compressed_size(&self) -> u64 {
        self.compressed_size
    }
}
