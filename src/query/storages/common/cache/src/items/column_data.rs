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

use arrow::array::ArrayRef;
use bytes::Bytes;
use databend_common_exception::ErrorCode;
use databend_common_expression::Column;

use crate::CachedItem;

pub struct ColumnData(Bytes);

impl ColumnData {
    pub fn from_bytes(bytes: Bytes) -> Self {
        ColumnData(bytes)
    }

    pub fn from_merge_io_read_result(bytes: Vec<u8>) -> Self {
        ColumnData(bytes.into())
    }

    pub fn bytes(&self) -> Bytes {
        self.0.clone()
    }

    pub fn size(&self) -> usize {
        self.0.len()
    }
}

impl TryFrom<&ColumnData> for Vec<u8> {
    type Error = ErrorCode;
    fn try_from(value: &ColumnData) -> Result<Self, Self::Error> {
        Ok(value.0.to_vec())
    }
}

// copying should be avoided
impl TryFrom<Bytes> for ColumnData {
    type Error = ErrorCode;

    fn try_from(value: Bytes) -> Result<Self, Self::Error> {
        Ok(ColumnData(value))
    }
}

/// A deserialized column and its uncompressed size in bytes.
pub type SizedColumnArray = (CachedColumnArray, usize);

/// A deserialized column kept in the in-memory column array cache.
#[derive(Clone, Debug)]
pub enum CachedColumnArray {
    /// Arrow array as produced by the Parquet reader.
    Arrow(ArrayRef),
    /// Column in Databend's representation, e.g. an encoded column that has no Arrow counterpart.
    Column(Column),
}

impl From<ArrayRef> for CachedColumnArray {
    fn from(array: ArrayRef) -> Self {
        CachedColumnArray::Arrow(array)
    }
}

impl CachedItem for ColumnData {
    fn mem_bytes(&self) -> usize {
        self.size()
    }
}

impl CachedItem for SizedColumnArray {
    fn mem_bytes(&self) -> usize {
        self.1
    }
}
