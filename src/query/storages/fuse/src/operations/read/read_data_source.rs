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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfo;
use databend_common_expression::DataBlock;

use super::native_data_source::NativeDataSource;
use super::parquet_data_source::ParquetDataSource;
use crate::operations::read::data_source_with_meta::DataSourceWithMeta;

pub enum ReadDataSource {
    Native(Box<NativeDataSource>),
    Parquet(Box<ParquetDataSource>),
    /// Vortex blocks are read and fully deserialized during the async IO phase.
    /// The deserializer just passes the DataBlock through.
    Vortex(DataBlock),
}

impl ReadDataSource {
    pub fn into_native(self) -> Result<NativeDataSource> {
        match self {
            ReadDataSource::Native(data) => Ok(*data),
            _ => Err(ErrorCode::Internal(
                "NativeDeserializeDataTransform got non-native data source",
            )),
        }
    }

    pub fn into_parquet(self) -> Result<ParquetDataSource> {
        match self {
            ReadDataSource::Parquet(data) => Ok(*data),
            _ => Err(ErrorCode::Internal(
                "DeserializeDataTransform got non-parquet data source",
            )),
        }
    }

    pub fn into_vortex(self) -> Result<DataBlock> {
        match self {
            ReadDataSource::Vortex(block) => Ok(block),
            _ => Err(ErrorCode::Internal(
                "VortexDeserializeDataTransform got non-vortex data source",
            )),
        }
    }
}

#[typetag::serde(name = "fuse_data_source")]
impl BlockMetaInfo for DataSourceWithMeta<ReadDataSource> {}
