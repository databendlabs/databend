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

//! This mod is the key point about compatibility.
//! Everytime update anything in this file, update the `VER` and let the tests pass.

use std::str::FromStr;

use databend_common_io::GeometryDataType;
use databend_common_meta_app as mt;
use databend_common_meta_app::principal::BinaryFormat;
use databend_common_meta_app::principal::EmptyFieldAs;
use databend_common_protos::pb;
use num::FromPrimitive;

use crate::reader_check_msg;
use crate::FromToProto;
use crate::FromToProtoEnum;
use crate::Incompatible;
use crate::MIN_READER_VER;
use crate::VER;

impl FromToProtoEnum for mt::principal::StageFileFormatType {
    type PBEnum = pb::StageFileFormatType;
    fn from_pb_enum(p: pb::StageFileFormatType) -> Result<Self, Incompatible>
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

    fn to_pb_enum(&self) -> Result<pb::StageFileFormatType, Incompatible> {
        match *self {
            mt::principal::StageFileFormatType::Csv => Ok(pb::StageFileFormatType::Csv),
            mt::principal::StageFileFormatType::Tsv => Ok(pb::StageFileFormatType::Tsv),
            mt::principal::StageFileFormatType::Json => Ok(pb::StageFileFormatType::Json),
            mt::principal::StageFileFormatType::NdJson => Ok(pb::StageFileFormatType::NdJson),
            mt::principal::StageFileFormatType::Avro => Ok(pb::StageFileFormatType::Avro),
            mt::principal::StageFileFormatType::Orc => Ok(pb::StageFileFormatType::Orc),
            mt::principal::StageFileFormatType::Parquet => Ok(pb::StageFileFormatType::Parquet),
            mt::principal::StageFileFormatType::Xml => Ok(pb::StageFileFormatType::Xml),
            mt::principal::StageFileFormatType::None => Err(Incompatible::new(
                "StageFileFormatType::None cannot be converted to protobuf".to_string(),
            )),
        }
    }
}

impl FromToProtoEnum for mt::principal::StageFileCompression {
    type PBEnum = pb::StageFileCompression;
    fn from_pb_enum(p: pb::StageFileCompression) -> Result<Self, Incompatible>
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

    fn to_pb_enum(&self) -> Result<pb::StageFileCompression, Incompatible> {
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

impl FromToProto for mt::principal::FileFormatOptions {
    type PB = pb::FileFormatOptions;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: pb::FileFormatOptions) -> Result<Self, Incompatible>
    where Self: Sized {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        let format = mt::principal::StageFileFormatType::from_pb_enum(
            FromPrimitive::from_i32(p.format).ok_or_else(|| {
                Incompatible::new(format!("invalid StageFileFormatType: {}", p.format))
            })?,
        )?;

        let compression = mt::principal::StageFileCompression::from_pb_enum(
            FromPrimitive::from_i32(p.compression).ok_or_else(|| {
                Incompatible::new(format!("invalid StageFileCompression: {}", p.compression))
            })?,
        )?;

        let nan_display = if p.nan_display.is_empty() {
            "".to_string()
        } else {
            p.nan_display
        };

        Ok(mt::principal::FileFormatOptions {
            format,
            skip_header: p.skip_header,
            field_delimiter: p.field_delimiter.clone(),
            record_delimiter: p.record_delimiter,
            nan_display,
            escape: p.escape,
            compression,
            row_tag: p.row_tag,
            quote: p.quote,
            name: None,
        })
    }

    fn to_pb(&self) -> Result<pb::FileFormatOptions, Incompatible> {
        let format = mt::principal::StageFileFormatType::to_pb_enum(&self.format)? as i32;
        let compression =
            mt::principal::StageFileCompression::to_pb_enum(&self.compression)? as i32;
        Ok(pb::FileFormatOptions {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            format,
            skip_header: self.skip_header,
            field_delimiter: self.field_delimiter.clone(),
            record_delimiter: self.record_delimiter.clone(),
            nan_display: self.nan_display.clone(),
            compression,
            row_tag: self.row_tag.clone(),
            escape: self.escape.clone(),
            quote: self.quote.clone(),
        })
    }
}

impl FromToProto for mt::principal::UserDefinedFileFormat {
    type PB = pb::UserDefinedFileFormat;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: pb::UserDefinedFileFormat) -> Result<Self, Incompatible>
    where Self: Sized {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        let file_format_params =
            mt::principal::FileFormatParams::from_pb(p.file_format_params.ok_or_else(|| {
                Incompatible::new("StageInfo.file_format_params cannot be None".to_string())
            })?)?;
        let creator =
            mt::principal::UserIdentity::from_pb(p.creator.ok_or_else(|| {
                Incompatible::new("StageInfo.creator cannot be None".to_string())
            })?)?;

        Ok(mt::principal::UserDefinedFileFormat {
            name: p.name,
            file_format_params,
            creator,
        })
    }

    fn to_pb(&self) -> Result<pb::UserDefinedFileFormat, Incompatible> {
        let file_format_params = mt::principal::FileFormatParams::to_pb(&self.file_format_params)?;
        let creator = mt::principal::UserIdentity::to_pb(&self.creator)?;
        Ok(pb::UserDefinedFileFormat {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            name: self.name.clone(),
            file_format_params: Some(file_format_params),
            creator: Some(creator),
        })
    }
}

impl FromToProto for mt::principal::FileFormatParams {
    type PB = pb::FileFormatParams;
    fn get_pb_ver(_p: &Self::PB) -> u64 {
        0
    }

    fn from_pb(p: Self::PB) -> Result<Self, Incompatible>
    where Self: Sized {
        match p.format {
            Some(pb::file_format_params::Format::Orc(p)) => {
                Ok(mt::principal::FileFormatParams::Orc(
                    mt::principal::OrcFileFormatParams::from_pb(p)?,
                ))
            }
            Some(pb::file_format_params::Format::Parquet(p)) => {
                Ok(mt::principal::FileFormatParams::Parquet(
                    mt::principal::ParquetFileFormatParams::from_pb(p)?,
                ))
            }
            Some(pb::file_format_params::Format::NdJson(p)) => {
                Ok(mt::principal::FileFormatParams::NdJson(
                    mt::principal::NdJsonFileFormatParams::from_pb(p)?,
                ))
            }
            Some(pb::file_format_params::Format::Csv(p)) => {
                Ok(mt::principal::FileFormatParams::Csv(
                    mt::principal::CsvFileFormatParams::from_pb(p)?,
                ))
            }
            Some(pb::file_format_params::Format::Json(p)) => {
                Ok(mt::principal::FileFormatParams::Json(
                    mt::principal::JsonFileFormatParams::from_pb(p)?,
                ))
            }
            Some(pb::file_format_params::Format::Tsv(p)) => {
                Ok(mt::principal::FileFormatParams::Tsv(
                    mt::principal::TsvFileFormatParams::from_pb(p)?,
                ))
            }
            Some(pb::file_format_params::Format::Xml(p)) => {
                Ok(mt::principal::FileFormatParams::Xml(
                    mt::principal::XmlFileFormatParams::from_pb(p)?,
                ))
            }
            Some(pb::file_format_params::Format::Avro(p)) => {
                Ok(mt::principal::FileFormatParams::Avro(
                    mt::principal::AvroFileFormatParams::from_pb(p)?,
                ))
            }
            None => Err(Incompatible::new(
                "FileFormatParams.format cannot be None".to_string(),
            )),
        }
    }

    fn to_pb(&self) -> Result<Self::PB, Incompatible> {
        match self {
            Self::Parquet(p) => Ok(Self::PB {
                format: Some(pb::file_format_params::Format::Parquet(
                    mt::principal::ParquetFileFormatParams::to_pb(p)?,
                )),
            }),
            Self::NdJson(p) => Ok(Self::PB {
                format: Some(pb::file_format_params::Format::NdJson(
                    mt::principal::NdJsonFileFormatParams::to_pb(p)?,
                )),
            }),
            Self::Csv(p) => Ok(Self::PB {
                format: Some(pb::file_format_params::Format::Csv(
                    mt::principal::CsvFileFormatParams::to_pb(p)?,
                )),
            }),
            Self::Json(p) => Ok(Self::PB {
                format: Some(pb::file_format_params::Format::Json(
                    mt::principal::JsonFileFormatParams::to_pb(p)?,
                )),
            }),
            Self::Avro(p) => Ok(Self::PB {
                format: Some(pb::file_format_params::Format::Avro(
                    mt::principal::AvroFileFormatParams::to_pb(p)?,
                )),
            }),
            Self::Tsv(p) => Ok(Self::PB {
                format: Some(pb::file_format_params::Format::Tsv(
                    mt::principal::TsvFileFormatParams::to_pb(p)?,
                )),
            }),
            Self::Xml(p) => Ok(Self::PB {
                format: Some(pb::file_format_params::Format::Xml(
                    mt::principal::XmlFileFormatParams::to_pb(p)?,
                )),
            }),
            Self::Orc(p) => Ok(Self::PB {
                format: Some(pb::file_format_params::Format::Orc(
                    mt::principal::OrcFileFormatParams::to_pb(p)?,
                )),
            }),
        }
    }
}

impl FromToProto for mt::principal::OrcFileFormatParams {
    type PB = pb::OrcFileFormatParams;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }

    fn from_pb(p: pb::OrcFileFormatParams) -> Result<Self, Incompatible>
    where Self: Sized {
        reader_check_msg(p.ver, p.min_reader_ver)?;
        mt::principal::OrcFileFormatParams::try_create(p.missing_field_as.as_deref())
            .map_err(|e| Incompatible::new(format!("{e}")))
    }

    fn to_pb(&self) -> Result<pb::OrcFileFormatParams, Incompatible> {
        Ok(pb::OrcFileFormatParams {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            missing_field_as: Some(self.missing_field_as.to_string()),
        })
    }
}

impl FromToProto for mt::principal::ParquetFileFormatParams {
    type PB = pb::ParquetFileFormatParams;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }

    fn from_pb(p: pb::ParquetFileFormatParams) -> Result<Self, Incompatible>
    where Self: Sized {
        reader_check_msg(p.ver, p.min_reader_ver)?;
        mt::principal::ParquetFileFormatParams::try_create(p.missing_field_as.as_deref(), p.null_if)
            .map_err(|e| Incompatible::new(format!("{e}")))
    }

    fn to_pb(&self) -> Result<pb::ParquetFileFormatParams, Incompatible> {
        Ok(pb::ParquetFileFormatParams {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            missing_field_as: Some(self.missing_field_as.to_string()),
            null_if: self.null_if.clone(),
        })
    }
}

impl FromToProto for mt::principal::NdJsonFileFormatParams {
    type PB = pb::NdJsonFileFormatParams;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }

    fn from_pb(p: pb::NdJsonFileFormatParams) -> Result<Self, Incompatible>
    where Self: Sized {
        reader_check_msg(p.ver, p.min_reader_ver)?;
        let compression = mt::principal::StageFileCompression::from_pb_enum(
            FromPrimitive::from_i32(p.compression).ok_or_else(|| {
                Incompatible::new(format!("invalid StageFileCompression: {}", p.compression))
            })?,
        )?;

        mt::principal::NdJsonFileFormatParams::try_create(
            compression,
            p.missing_field_as.as_deref(),
            p.null_field_as.as_deref(),
            p.null_if,
        )
        .map_err(|e| Incompatible::new(format!("{e}")))
    }

    fn to_pb(&self) -> Result<pb::NdJsonFileFormatParams, Incompatible> {
        let compression =
            mt::principal::StageFileCompression::to_pb_enum(&self.compression)? as i32;
        Ok(pb::NdJsonFileFormatParams {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            compression,
            missing_field_as: Some(self.missing_field_as.to_string()),
            null_field_as: Some(self.null_field_as.to_string()),
            null_if: self.null_if.clone(),
        })
    }
}

impl FromToProto for mt::principal::JsonFileFormatParams {
    type PB = pb::JsonFileFormatParams;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }

    fn from_pb(p: Self::PB) -> Result<Self, Incompatible>
    where Self: Sized {
        reader_check_msg(p.ver, p.min_reader_ver)?;
        let compression = mt::principal::StageFileCompression::from_pb_enum(
            FromPrimitive::from_i32(p.compression).ok_or_else(|| {
                Incompatible::new(format!("invalid StageFileCompression: {}", p.compression))
            })?,
        )?;
        Ok(Self { compression })
    }

    fn to_pb(&self) -> Result<Self::PB, Incompatible> {
        let compression =
            mt::principal::StageFileCompression::to_pb_enum(&self.compression)? as i32;
        Ok(Self::PB {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            compression,
        })
    }
}

impl FromToProto for mt::principal::XmlFileFormatParams {
    type PB = pb::XmlFileFormatParams;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }

    fn from_pb(p: Self::PB) -> Result<Self, Incompatible>
    where Self: Sized {
        reader_check_msg(p.ver, p.min_reader_ver)?;
        let compression = mt::principal::StageFileCompression::from_pb_enum(
            FromPrimitive::from_i32(p.compression).ok_or_else(|| {
                Incompatible::new(format!("invalid StageFileCompression: {}", p.compression))
            })?,
        )?;
        Ok(Self {
            compression,
            row_tag: p.row_tag,
        })
    }

    fn to_pb(&self) -> Result<Self::PB, Incompatible> {
        let compression =
            mt::principal::StageFileCompression::to_pb_enum(&self.compression)? as i32;
        Ok(Self::PB {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            compression,
            row_tag: self.row_tag.clone(),
        })
    }
}

impl FromToProto for mt::principal::CsvFileFormatParams {
    type PB = pb::CsvFileFormatParams;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }

    fn from_pb(p: Self::PB) -> Result<Self, Incompatible>
    where Self: Sized {
        reader_check_msg(p.ver, p.min_reader_ver)?;
        let compression = mt::principal::StageFileCompression::from_pb_enum(
            FromPrimitive::from_i32(p.compression).ok_or_else(|| {
                Incompatible::new(format!("invalid StageFileCompression: {}", p.compression))
            })?,
        )?;
        let null_display = if p.null_display.is_empty() {
            "\\N".to_string()
        } else {
            p.null_display
        };

        let empty_field_as = p
            .empty_field_as
            .map(|s| EmptyFieldAs::from_str(&s).map_err(|e| Incompatible::new(format!("{:?}", e))))
            .transpose()?
            .unwrap_or_default();

        let binary_format = p
            .binary_format
            .map(|s| BinaryFormat::from_str(&s))
            .transpose()
            .map_err(|e| Incompatible::new(format!("{:?}", e)))?
            .unwrap_or_default();
        let geometry_format = p
            .geometry_format
            .map(|s| GeometryDataType::from_str(&s))
            .transpose()
            .map_err(|e| Incompatible::new(format!("{:?}", e)))?
            .unwrap_or_default();

        Ok(Self {
            compression,
            headers: p.headers,
            field_delimiter: p.field_delimiter,
            record_delimiter: p.record_delimiter,
            quote: p.quote,
            escape: p.escape,
            nan_display: p.nan_display,
            null_display,
            error_on_column_count_mismatch: !p.allow_column_count_mismatch,
            empty_field_as,
            binary_format,
            output_header: p.output_header,
            geometry_format,
        })
    }

    fn to_pb(&self) -> Result<Self::PB, Incompatible> {
        let compression =
            mt::principal::StageFileCompression::to_pb_enum(&self.compression)? as i32;
        Ok(Self::PB {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            compression,
            headers: self.headers,
            field_delimiter: self.field_delimiter.clone(),
            record_delimiter: self.record_delimiter.clone(),
            quote: self.quote.clone(),
            escape: self.escape.clone(),
            nan_display: self.nan_display.clone(),
            null_display: self.null_display.clone(),
            allow_column_count_mismatch: !self.error_on_column_count_mismatch,
            empty_field_as: Some(self.empty_field_as.to_string()),
            binary_format: Some(self.binary_format.to_string()),
            output_header: self.output_header,
            geometry_format: Some(self.geometry_format.to_string()),
        })
    }
}

impl FromToProto for mt::principal::TsvFileFormatParams {
    type PB = pb::TsvFileFormatParams;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }

    fn from_pb(p: Self::PB) -> Result<Self, Incompatible>
    where Self: Sized {
        reader_check_msg(p.ver, p.min_reader_ver)?;
        let compression = mt::principal::StageFileCompression::from_pb_enum(
            FromPrimitive::from_i32(p.compression).ok_or_else(|| {
                Incompatible::new(format!("invalid StageFileCompression: {}", p.compression))
            })?,
        )?;
        Ok(Self {
            compression,
            headers: p.headers,
            field_delimiter: p.field_delimiter,
            record_delimiter: p.record_delimiter,
            escape: p.escape,
            nan_display: p.nan_display,
            quote: p.quote,
        })
    }

    fn to_pb(&self) -> Result<Self::PB, Incompatible> {
        let compression =
            mt::principal::StageFileCompression::to_pb_enum(&self.compression)? as i32;
        Ok(Self::PB {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            compression,
            headers: self.headers,
            field_delimiter: self.field_delimiter.clone(),
            record_delimiter: self.record_delimiter.clone(),
            escape: self.escape.clone(),
            quote: self.quote.clone(),
            nan_display: self.nan_display.clone(),
        })
    }
}

impl FromToProto for mt::principal::AvroFileFormatParams {
    type PB = pb::AvroFileFormatParams;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }

    fn from_pb(p: pb::AvroFileFormatParams) -> Result<Self, Incompatible>
    where Self: Sized {
        reader_check_msg(p.ver, p.min_reader_ver)?;
        let compression = mt::principal::StageFileCompression::from_pb_enum(
            FromPrimitive::from_i32(p.compression).ok_or_else(|| {
                Incompatible::new(format!("invalid StageFileCompression: {}", p.compression))
            })?,
        )?;

        mt::principal::AvroFileFormatParams::try_create(
            compression,
            p.missing_field_as.as_deref(),
            p.null_if,
        )
        .map_err(|e| Incompatible::new(format!("{e}")))
    }

    fn to_pb(&self) -> Result<pb::AvroFileFormatParams, Incompatible> {
        let compression =
            mt::principal::StageFileCompression::to_pb_enum(&self.compression)? as i32;
        Ok(pb::AvroFileFormatParams {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            compression,
            missing_field_as: Some(self.missing_field_as.to_string()),
            null_if: self.null_if.clone(),
        })
    }
}
