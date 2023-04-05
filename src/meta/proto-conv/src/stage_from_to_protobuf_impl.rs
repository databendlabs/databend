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

use std::convert::TryFrom;

use chrono::DateTime;
use chrono::Utc;
use common_meta_app as mt;
use common_protos::pb;
use num::FromPrimitive;

use crate::reader_check_msg;
use crate::FromToProto;
use crate::Incompatible;
use crate::MIN_READER_VER;
use crate::VER;

impl FromToProto for mt::principal::StageParams {
    type PB = pb::stage_info::StageParams;
    fn get_pb_ver(_p: &Self::PB) -> u64 {
        0
    }
    fn from_pb(p: pb::stage_info::StageParams) -> Result<Self, Incompatible>
    where Self: Sized {
        Ok(mt::principal::StageParams {
            storage: mt::storage::StorageParams::from_pb(p.storage.ok_or_else(|| {
                Incompatible {
                    reason: "pb::stage_info::StageParams.storage cannot be None".to_string(),
                }
            })?)?,
        })
    }

    fn to_pb(&self) -> Result<pb::stage_info::StageParams, Incompatible> {
        Ok(pb::stage_info::StageParams {
            storage: Some(mt::storage::StorageParams::to_pb(&self.storage)?),
        })
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

        let format = mt::principal::StageFileFormatType::from_pb(
            FromPrimitive::from_i32(p.format).ok_or_else(|| Incompatible {
                reason: format!("invalid StageFileFormatType: {}", p.format),
            })?,
        )?;

        let compression = mt::principal::StageFileCompression::from_pb(
            FromPrimitive::from_i32(p.compression).ok_or_else(|| Incompatible {
                reason: format!("invalid StageFileCompression: {}", p.compression),
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
        let format = mt::principal::StageFileFormatType::to_pb(&self.format)? as i32;
        let compression = mt::principal::StageFileCompression::to_pb(&self.compression)? as i32;
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

        let file_format_options =
            mt::principal::FileFormatOptions::from_pb(p.file_format_options.ok_or_else(|| {
                Incompatible {
                    reason: "StageInfo.file_format_options cannot be None".to_string(),
                }
            })?)?;
        let creator =
            mt::principal::UserIdentity::from_pb(p.creator.ok_or_else(|| Incompatible {
                reason: "StageInfo.file_format_options cannot be None".to_string(),
            })?)?;

        Ok(mt::principal::UserDefinedFileFormat {
            name: p.name,
            file_format_options,
            creator,
        })
    }

    fn to_pb(&self) -> Result<pb::UserDefinedFileFormat, Incompatible> {
        let file_format_options =
            mt::principal::FileFormatOptions::to_pb(&self.file_format_options)?;
        let creator = mt::principal::UserIdentity::to_pb(&self.creator)?;
        Ok(pb::UserDefinedFileFormat {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            name: self.name.clone(),
            file_format_options: Some(file_format_options),
            creator: Some(creator),
        })
    }
}

impl FromToProto for mt::principal::OnErrorMode {
    type PB = pb::stage_info::OnErrorMode;
    fn get_pb_ver(_p: &Self::PB) -> u64 {
        0
    }
    fn from_pb(p: pb::stage_info::OnErrorMode) -> Result<Self, Incompatible>
    where Self: Sized {
        match p.mode {
            Some(pb::stage_info::on_error_mode::Mode::None(_))
            | Some(pb::stage_info::on_error_mode::Mode::AbortStatement(_)) => {
                Ok(mt::principal::OnErrorMode::AbortNum(1))
            }
            Some(pb::stage_info::on_error_mode::Mode::Continue(_)) => {
                Ok(mt::principal::OnErrorMode::Continue)
            }
            Some(pb::stage_info::on_error_mode::Mode::SkipFile(_)) => {
                Ok(mt::principal::OnErrorMode::SkipFileNum(1))
            }
            Some(pb::stage_info::on_error_mode::Mode::SkipFileNum(n)) => {
                Ok(mt::principal::OnErrorMode::SkipFileNum(n))
            }
            Some(pb::stage_info::on_error_mode::Mode::AbortNum(n)) => {
                Ok(mt::principal::OnErrorMode::AbortNum(n))
            }
            None => Err(Incompatible {
                reason: "OnErrorMode.mode cannot be None".to_string(),
            }),
        }
    }

    fn to_pb(&self) -> Result<pb::stage_info::OnErrorMode, Incompatible> {
        match self {
            mt::principal::OnErrorMode::Continue => Ok(pb::stage_info::OnErrorMode {
                mode: Some(pb::stage_info::on_error_mode::Mode::Continue(pb::Empty {})),
            }),
            mt::principal::OnErrorMode::SkipFileNum(n) => Ok(pb::stage_info::OnErrorMode {
                mode: Some(pb::stage_info::on_error_mode::Mode::SkipFileNum(*n)),
            }),
            mt::principal::OnErrorMode::AbortNum(n) => Ok(pb::stage_info::OnErrorMode {
                mode: Some(pb::stage_info::on_error_mode::Mode::AbortNum(*n)),
            }),
        }
    }
}

impl FromToProto for mt::principal::CopyOptions {
    type PB = pb::stage_info::CopyOptions;
    fn get_pb_ver(_p: &Self::PB) -> u64 {
        0
    }
    fn from_pb(p: pb::stage_info::CopyOptions) -> Result<Self, Incompatible>
    where Self: Sized {
        let on_error =
            mt::principal::OnErrorMode::from_pb(p.on_error.ok_or_else(|| Incompatible {
                reason: "CopyOptions.on_error cannot be None".to_string(),
            })?)?;
        let size_limit = usize::try_from(p.size_limit).map_err(|err| Incompatible {
            reason: format!("CopyOptions.size_limit cannot be convert to usize: {}", err),
        })?;
        let max_files = usize::try_from(p.max_files).map_err(|err| Incompatible {
            reason: format!("CopyOptions.max_files cannot be convert to usize: {}", err),
        })?;
        let split_size = usize::try_from(p.split_size).map_err(|err| Incompatible {
            reason: format!("CopyOptions.split_size cannot be convert to usize: {}", err),
        })?;

        let max_file_size = usize::try_from(p.max_file_size).map_err(|err| Incompatible {
            reason: format!(
                "CopyOptions.max_file_size cannot be convert to usize: {}",
                err
            ),
        })?;
        Ok(mt::principal::CopyOptions {
            on_error,
            size_limit,
            max_files,
            split_size,
            purge: p.purge,
            single: p.single,
            max_file_size,
        })
    }

    fn to_pb(&self) -> Result<pb::stage_info::CopyOptions, Incompatible> {
        let on_error = mt::principal::OnErrorMode::to_pb(&self.on_error)?;
        let size_limit = u64::try_from(self.size_limit).map_err(|err| Incompatible {
            reason: format!("CopyOptions.size_limit cannot be convert to u64: {}", err),
        })?;
        let max_files = u64::try_from(self.max_files).map_err(|err| Incompatible {
            reason: format!("CopyOptions.max_files cannot be convert to u64: {}", err),
        })?;
        let split_size = u64::try_from(self.split_size).map_err(|err| Incompatible {
            reason: format!("CopyOptions.split_size cannot be convert to u64: {}", err),
        })?;
        let max_file_size = u64::try_from(self.max_file_size).map_err(|err| Incompatible {
            reason: format!(
                "CopyOptions.max_file_size cannot be convert to u64: {}",
                err
            ),
        })?;
        Ok(pb::stage_info::CopyOptions {
            on_error: Some(on_error),
            size_limit,
            max_files,
            split_size,
            purge: self.purge,
            single: self.single,
            max_file_size,
        })
    }
}

impl FromToProto for mt::principal::StageInfo {
    type PB = pb::StageInfo;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: pb::StageInfo) -> Result<Self, Incompatible>
    where Self: Sized {
        reader_check_msg(p.ver, p.min_reader_ver)?;
        Ok(mt::principal::StageInfo {
            stage_name: p.stage_name.clone(),
            stage_type: mt::principal::StageType::from_pb(
                FromPrimitive::from_i32(p.stage_type).ok_or_else(|| Incompatible {
                    reason: format!("invalid StageType: {}", p.stage_type),
                })?,
            )?,
            stage_params: mt::principal::StageParams::from_pb(p.stage_params.ok_or_else(
                || Incompatible {
                    reason: "StageInfo.stage_params cannot be None".to_string(),
                },
            )?)?,
            file_format_options: mt::principal::FileFormatOptions::from_pb(
                p.file_format_options.ok_or_else(|| Incompatible {
                    reason: "StageInfo.file_format_options cannot be None".to_string(),
                })?,
            )?,
            copy_options: mt::principal::CopyOptions::from_pb(p.copy_options.ok_or_else(
                || Incompatible {
                    reason: "StageInfo.copy_options cannot be None".to_string(),
                },
            )?)?,
            comment: p.comment,
            number_of_files: p.number_of_files,
            creator: match p.creator {
                Some(c) => Some(mt::principal::UserIdentity::from_pb(c)?),
                None => None,
            },
        })
    }

    fn to_pb(&self) -> Result<pb::StageInfo, Incompatible> {
        Ok(pb::StageInfo {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            stage_name: self.stage_name.clone(),
            stage_type: mt::principal::StageType::to_pb(&self.stage_type)? as i32,
            stage_params: Some(mt::principal::StageParams::to_pb(&self.stage_params)?),
            file_format_options: Some(mt::principal::FileFormatOptions::to_pb(
                &self.file_format_options,
            )?),
            copy_options: Some(mt::principal::CopyOptions::to_pb(&self.copy_options)?),
            comment: self.comment.clone(),
            number_of_files: self.number_of_files,
            creator: match &self.creator {
                Some(c) => Some(mt::principal::UserIdentity::to_pb(c)?),
                None => None,
            },
        })
    }
}

impl FromToProto for mt::principal::StageFile {
    type PB = pb::StageFile;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: pb::StageFile) -> Result<Self, Incompatible>
    where Self: Sized {
        reader_check_msg(p.ver, p.min_reader_ver)?;
        Ok(mt::principal::StageFile {
            path: p.path.clone(),
            size: p.size,
            md5: p.md5.clone(),
            last_modified: DateTime::<Utc>::from_pb(p.last_modified)?,
            creator: match p.creator {
                Some(c) => Some(mt::principal::UserIdentity::from_pb(c)?),
                None => None,
            },
            etag: p.etag.clone(),
        })
    }

    fn to_pb(&self) -> Result<pb::StageFile, Incompatible> {
        Ok(pb::StageFile {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            path: self.path.clone(),
            size: self.size,
            md5: self.md5.clone(),
            last_modified: self.last_modified.to_pb()?,
            creator: match &self.creator {
                Some(c) => Some(mt::principal::UserIdentity::to_pb(c)?),
                None => None,
            },
            etag: self.etag.clone(),
        })
    }
}
