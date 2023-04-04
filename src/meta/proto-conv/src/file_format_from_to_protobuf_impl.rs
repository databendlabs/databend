// Copyright 2023 Datafuse Labs.
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

//! This mod is the key point about compatibility.
//! Everytime update anything in this file, update the `VER` and let the tests pass.

use common_meta_app as mt;
use common_protos::pb;

use crate::FromToProto;
use crate::Incompatible;

impl FromToProto for mt::principal::StageFileFormatType {
    type PB = pb::StageFileFormatType;
    fn get_pb_ver(_p: &Self::PB) -> u64 {
        0
    }
    fn from_pb(p: pb::StageFileFormatType) -> Result<Self, Incompatible>
    where Self: Sized {
        match p {
            pb::StageFileFormatType::Csv => Ok(mt::principal::StageFileFormatType::Csv),
            pb::StageFileFormatType::Tsv => Ok(mt::principal::StageFileFormatType::Tsv),
            pb::StageFileFormatType::Json => Ok(mt::principal::StageFileFormatType::Json),
            pb::StageFileFormatType::NdJson => Ok(mt::principal::StageFileFormatType::NdJson),
            pb::StageFileFormatType::Avro => Ok(mt::principal::StageFileFormatType::Avro),
            pb::StageFileFormatType::Orc => Ok(mt::principal::StageFileFormatType::Orc),
            pb::StageFileFormatType::Parquet => Ok(mt::principal::StageFileFormatType::Parquet),
            pb::StageFileFormatType::Xml => Ok(mt::principal::StageFileFormatType::Xml),
        }
    }

    fn to_pb(&self) -> Result<pb::StageFileFormatType, Incompatible> {
        match *self {
            mt::principal::StageFileFormatType::Csv => Ok(pb::StageFileFormatType::Csv),
            mt::principal::StageFileFormatType::Tsv => Ok(pb::StageFileFormatType::Tsv),
            mt::principal::StageFileFormatType::Json => Ok(pb::StageFileFormatType::Json),
            mt::principal::StageFileFormatType::NdJson => Ok(pb::StageFileFormatType::NdJson),
            mt::principal::StageFileFormatType::Avro => Ok(pb::StageFileFormatType::Avro),
            mt::principal::StageFileFormatType::Orc => Ok(pb::StageFileFormatType::Orc),
            mt::principal::StageFileFormatType::Parquet => Ok(pb::StageFileFormatType::Parquet),
            mt::principal::StageFileFormatType::Xml => Ok(pb::StageFileFormatType::Xml),
            mt::principal::StageFileFormatType::None => Err(Incompatible {
                reason: "StageFileFormatType::None cannot be converted to protobuf".to_string(),
            }),
        }
    }
}

impl FromToProto for mt::principal::StageFileCompression {
    type PB = pb::StageFileCompression;
    fn get_pb_ver(_p: &Self::PB) -> u64 {
        0
    }
    fn from_pb(p: pb::StageFileCompression) -> Result<Self, Incompatible>
    where Self: Sized {
        match p {
            pb::StageFileCompression::Auto => Ok(mt::principal::StageFileCompression::Auto),
            pb::StageFileCompression::Gzip => Ok(mt::principal::StageFileCompression::Gzip),
            pb::StageFileCompression::Bz2 => Ok(mt::principal::StageFileCompression::Bz2),
            pb::StageFileCompression::Brotli => Ok(mt::principal::StageFileCompression::Brotli),
            pb::StageFileCompression::Zstd => Ok(mt::principal::StageFileCompression::Zstd),
            pb::StageFileCompression::Deflate => Ok(mt::principal::StageFileCompression::Deflate),
            pb::StageFileCompression::RawDeflate => {
                Ok(mt::principal::StageFileCompression::RawDeflate)
            }
            pb::StageFileCompression::Lzo => Ok(mt::principal::StageFileCompression::Lzo),
            pb::StageFileCompression::Snappy => Ok(mt::principal::StageFileCompression::Snappy),
            pb::StageFileCompression::None => Ok(mt::principal::StageFileCompression::None),
            pb::StageFileCompression::Xz => Ok(mt::principal::StageFileCompression::Xz),
        }
    }

    fn to_pb(&self) -> Result<pb::StageFileCompression, Incompatible> {
        match *self {
            mt::principal::StageFileCompression::Auto => Ok(pb::StageFileCompression::Auto),
            mt::principal::StageFileCompression::Gzip => Ok(pb::StageFileCompression::Gzip),
            mt::principal::StageFileCompression::Bz2 => Ok(pb::StageFileCompression::Bz2),
            mt::principal::StageFileCompression::Brotli => Ok(pb::StageFileCompression::Brotli),
            mt::principal::StageFileCompression::Zstd => Ok(pb::StageFileCompression::Zstd),
            mt::principal::StageFileCompression::Deflate => Ok(pb::StageFileCompression::Deflate),
            mt::principal::StageFileCompression::RawDeflate => {
                Ok(pb::StageFileCompression::RawDeflate)
            }
            mt::principal::StageFileCompression::Lzo => Ok(pb::StageFileCompression::Lzo),
            mt::principal::StageFileCompression::Snappy => Ok(pb::StageFileCompression::Snappy),
            mt::principal::StageFileCompression::None => Ok(pb::StageFileCompression::None),
            mt::principal::StageFileCompression::Xz => Ok(pb::StageFileCompression::Xz),
        }
    }
}
