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

use chrono::DateTime;
use chrono::Utc;
use databend_common_meta_app as mt;
use databend_common_protos::pb;
use mt::principal::FileFormatOptionsReader;
use num::FromPrimitive;

use crate::FromProtoOptionExt;
use crate::FromToProto;
use crate::FromToProtoEnum;
use crate::Incompatible;
use crate::MIN_READER_VER;
use crate::ToProtoOptionExt;
use crate::VER;
use crate::convert_field;
use crate::reader_check_msg;

impl FromToProto for mt::principal::StageParams {
    type PB = pb::stage_info::StageParams;
    fn get_pb_ver(_p: &Self::PB) -> u64 {
        0
    }
    fn from_pb(p: pb::stage_info::StageParams) -> Result<Self, Incompatible>
    where Self: Sized {
        Ok(mt::principal::StageParams {
            storage: mt::storage::StorageParams::from_pb(p.storage.ok_or_else(|| {
                Incompatible::new("pb::stage_info::StageParams.storage cannot be None".to_string())
            })?)?,
        })
    }

    fn to_pb(&self) -> Result<pb::stage_info::StageParams, Incompatible> {
        Ok(pb::stage_info::StageParams {
            storage: Some(mt::storage::StorageParams::to_pb(&self.storage)?),
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
            None => Err(Incompatible::new(
                "OnErrorMode.mode cannot be None".to_string(),
            )),
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
        let on_error = mt::principal::OnErrorMode::from_pb(p.on_error.ok_or_else(|| {
            Incompatible::new("CopyOptions.on_error cannot be None".to_string())
        })?)?;
        let size_limit: usize = convert_field(p.size_limit, "CopyOptions.size_limit")?;
        let max_files: usize = convert_field(p.max_files, "CopyOptions.max_files")?;
        let split_size: usize = convert_field(p.split_size, "CopyOptions.split_size")?;
        let max_file_size: usize = convert_field(p.max_file_size, "CopyOptions.max_file_size")?;
        Ok(mt::principal::CopyOptions {
            on_error,
            size_limit,
            max_files,
            split_size,
            purge: p.purge,
            single: p.single,
            max_file_size,
            disable_variant_check: p.disable_variant_check,
            return_failed_only: p.return_failed_only,
            detailed_output: false,
        })
    }

    fn to_pb(&self) -> Result<pb::stage_info::CopyOptions, Incompatible> {
        let on_error = mt::principal::OnErrorMode::to_pb(&self.on_error)?;
        let size_limit: u64 = convert_field(self.size_limit, "CopyOptions.size_limit")?;
        let max_files: u64 = convert_field(self.max_files, "CopyOptions.max_files")?;
        let split_size: u64 = convert_field(self.split_size, "CopyOptions.split_size")?;
        let max_file_size: u64 = convert_field(self.max_file_size, "CopyOptions.max_file_size")?;
        Ok(pb::stage_info::CopyOptions {
            on_error: Some(on_error),
            size_limit,
            max_files,
            split_size,
            purge: self.purge,
            single: self.single,
            max_file_size,
            disable_variant_check: self.disable_variant_check,
            return_failed_only: self.return_failed_only,
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
        let file_format_params = match  (p.file_format_params, p.file_format_options) {
            (Some(p), None) => {
                mt::principal::FileFormatParams::from_pb(p)?
            },
            (None, Some(p)) => {
                let options = mt::principal::FileFormatOptions::from_pb(p)?;
                let reader = FileFormatOptionsReader::from_map(options.to_map());
                mt::principal::FileFormatParams::try_from_reader(reader, true).map_err(|e| Incompatible::new(format!("fail to convert StageInfo.file_format_options to StageInfo.file_format_params: {e:?}")))?
            },
            (None, None) => Err(Incompatible::new("StageInfo.file_format_params and StageInfo.file_format_options cannot be both None".to_string()))?,
            (Some(_), Some(_)) => Err(Incompatible::new("StageInfo.file_format_params and StageInfo.file_format_options cannot be both set".to_string()))?,
        };
        Ok(mt::principal::StageInfo {
            stage_name: p.stage_name.clone(),
            stage_type: mt::principal::StageType::from_pb_enum(
                FromPrimitive::from_i32(p.stage_type).ok_or_else(|| {
                    Incompatible::new(format!("invalid StageType: {}", p.stage_type))
                })?,
            )?,
            stage_params: mt::principal::StageParams::from_pb(p.stage_params.ok_or_else(
                || Incompatible::new("StageInfo.stage_params cannot be None".to_string()),
            )?)?,
            is_temporary: false,
            allow_credential_chain: false,
            file_format_params,
            copy_options: mt::principal::CopyOptions::from_pb(p.copy_options.ok_or_else(
                || Incompatible::new("StageInfo.copy_options cannot be None".to_string()),
            )?)?,
            comment: p.comment,
            number_of_files: p.number_of_files,
            creator: p.creator.from_pb_opt()?,
            created_on: p
                .created_on
                .map(FromToProto::from_pb)
                .transpose()?
                .unwrap_or_default(),
        })
    }

    fn to_pb(&self) -> Result<pb::StageInfo, Incompatible> {
        Ok(pb::StageInfo {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            stage_name: self.stage_name.clone(),
            stage_type: mt::principal::StageType::to_pb_enum(&self.stage_type)? as i32,
            stage_params: Some(mt::principal::StageParams::to_pb(&self.stage_params)?),
            file_format_params: Some(mt::principal::FileFormatParams::to_pb(
                &self.file_format_params,
            )?),
            file_format_options: None,
            copy_options: Some(mt::principal::CopyOptions::to_pb(&self.copy_options)?),
            comment: self.comment.clone(),
            number_of_files: self.number_of_files,
            creator: self.creator.to_pb_opt()?,
            created_on: Some(self.created_on.to_pb()?),
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
            creator: p.creator.from_pb_opt()?,
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
            creator: self.creator.to_pb_opt()?,
            etag: self.etag.clone(),
        })
    }
}

impl FromToProtoEnum for mt::principal::StageType {
    type PBEnum = pb::stage_info::StageType;
    fn from_pb_enum(p: pb::stage_info::StageType) -> Result<Self, Incompatible>
    where Self: Sized {
        match p {
            pb::stage_info::StageType::LegacyInternal => {
                Ok(mt::principal::StageType::LegacyInternal)
            }
            pb::stage_info::StageType::External => Ok(mt::principal::StageType::External),
            pb::stage_info::StageType::Internal => Ok(mt::principal::StageType::Internal),
            pb::stage_info::StageType::User => Ok(mt::principal::StageType::User),
        }
    }

    fn to_pb_enum(&self) -> Result<pb::stage_info::StageType, Incompatible> {
        match *self {
            mt::principal::StageType::LegacyInternal => {
                Ok(pb::stage_info::StageType::LegacyInternal)
            }
            mt::principal::StageType::External => Ok(pb::stage_info::StageType::External),
            mt::principal::StageType::Internal => Ok(pb::stage_info::StageType::Internal),
            mt::principal::StageType::User => Ok(pb::stage_info::StageType::User),
        }
    }
}
