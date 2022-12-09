// Copyright 2022 Datafuse Labs.
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

use std::any::Any;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::Hash;
use std::hash::Hasher;
use std::sync::Arc;

use common_arrow::parquet::compression::Compression as ParquetCompression;
use common_catalog::plan::PartInfo;
use common_catalog::plan::PartInfoPtr;
use common_exception::ErrorCode;
use common_exception::Result;

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct ParquetLocationPart {
    pub location: String,
}

#[typetag::serde(name = "parquet_location")]
impl PartInfo for ParquetLocationPart {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn equals(&self, info: &Box<dyn PartInfo>) -> bool {
        match info.as_any().downcast_ref::<ParquetLocationPart>() {
            None => false,
            Some(other) => self == other,
        }
    }

    fn hash(&self) -> u64 {
        let mut s = DefaultHasher::new();
        self.location.hash(&mut s);
        s.finish()
    }
}

impl ParquetLocationPart {
    pub fn create(location: String) -> Arc<Box<dyn PartInfo>> {
        Arc::new(Box::new(ParquetLocationPart { location }))
    }

    pub fn from_part(info: &PartInfoPtr) -> Result<&ParquetLocationPart> {
        match info.as_any().downcast_ref::<ParquetLocationPart>() {
            Some(part_ref) => Ok(part_ref),
            None => Err(ErrorCode::Internal(
                "Cannot downcast from PartInfo to ParquetLocationPart.",
            )),
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Eq, PartialEq, Hash, Clone, Copy)]
pub enum Compression {
    Uncompressed,
    Snappy,
    Gzip,
    Lzo,
    Brotli,
    Lz4,
    Zstd,
    Lz4Raw,
}

impl From<Compression> for ParquetCompression {
    fn from(value: Compression) -> Self {
        match value {
            Compression::Uncompressed => ParquetCompression::Uncompressed,
            Compression::Snappy => ParquetCompression::Snappy,
            Compression::Gzip => ParquetCompression::Gzip,
            Compression::Lzo => ParquetCompression::Lzo,
            Compression::Brotli => ParquetCompression::Brotli,
            Compression::Lz4 => ParquetCompression::Lz4,
            Compression::Zstd => ParquetCompression::Zstd,
            Compression::Lz4Raw => ParquetCompression::Lz4Raw,
        }
    }
}

impl From<ParquetCompression> for Compression {
    fn from(value: ParquetCompression) -> Self {
        match value {
            ParquetCompression::Uncompressed => Compression::Uncompressed,
            ParquetCompression::Snappy => Compression::Snappy,
            ParquetCompression::Gzip => Compression::Gzip,
            ParquetCompression::Lzo => Compression::Lzo,
            ParquetCompression::Brotli => Compression::Brotli,
            ParquetCompression::Lz4 => Compression::Lz4,
            ParquetCompression::Zstd => Compression::Zstd,
            ParquetCompression::Lz4Raw => Compression::Lz4Raw,
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct ColumnMeta {
    pub offset: u64,
    pub length: u64,
    pub compression: Compression,
}

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct ParquetRowGroupPart {
    pub location: String,
    pub num_rows: usize,
    pub column_metas: HashMap<usize, ColumnMeta>,
}

#[typetag::serde(name = "parquet_row_group")]
impl PartInfo for ParquetRowGroupPart {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn equals(&self, info: &Box<dyn PartInfo>) -> bool {
        match info.as_any().downcast_ref::<ParquetRowGroupPart>() {
            None => false,
            Some(other) => self == other,
        }
    }

    fn hash(&self) -> u64 {
        let mut s = DefaultHasher::new();
        self.location.hash(&mut s);
        s.finish()
    }
}

impl ParquetRowGroupPart {
    pub fn create(
        location: String,
        num_rows: usize,
        column_metas: HashMap<usize, ColumnMeta>,
    ) -> Arc<Box<dyn PartInfo>> {
        Arc::new(Box::new(ParquetRowGroupPart {
            location,
            num_rows,
            column_metas,
        }))
    }

    pub fn from_part(info: &PartInfoPtr) -> Result<&ParquetRowGroupPart> {
        match info.as_any().downcast_ref::<ParquetRowGroupPart>() {
            Some(part_ref) => Ok(part_ref),
            None => Err(ErrorCode::Internal(
                "Cannot downcast from PartInfo to ParquetRowGroupPart.",
            )),
        }
    }
}
