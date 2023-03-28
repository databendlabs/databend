// Copyright 2021 Datafuse Labs.
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

//! This mod is the key point about `User` compatibility.
//! Everytime update anything in this file, update the `VER` and let the tests pass.

use std::collections::BTreeMap;
use std::collections::HashSet;
use std::convert::TryFrom;

use chrono::DateTime;
use chrono::Utc;
use common_meta_app as mt;
use common_protos::pb;
use enumflags2::BitFlags;
use num::FromPrimitive;

use crate::reader_check_msg;
use crate::FromToProto;
use crate::Incompatible;
use crate::MIN_READER_VER;
use crate::VER;

impl FromToProto for mt::principal::AuthInfo {
    type PB = pb::AuthInfo;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: pb::AuthInfo) -> Result<Self, Incompatible>
    where Self: Sized {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        match p.info {
            Some(pb::auth_info::Info::None(pb::auth_info::None {})) => {
                Ok(mt::principal::AuthInfo::None)
            }
            Some(pb::auth_info::Info::Jwt(pb::auth_info::Jwt {})) => {
                Ok(mt::principal::AuthInfo::JWT)
            }
            Some(pb::auth_info::Info::Password(pb::auth_info::Password {
                hash_value,
                hash_method,
            })) => Ok(mt::principal::AuthInfo::Password {
                hash_value,
                hash_method: FromPrimitive::from_i32(hash_method).ok_or_else(|| Incompatible {
                    reason: format!("invalid PasswordHashMethod: {}", hash_method),
                })?,
            }),
            None => Err(Incompatible {
                reason: "AuthInfo cannot be None".to_string(),
            }),
        }
    }

    fn to_pb(&self) -> Result<pb::AuthInfo, Incompatible> {
        let info = match self {
            mt::principal::AuthInfo::None => {
                Some(pb::auth_info::Info::None(pb::auth_info::None {}))
            }
            mt::principal::AuthInfo::JWT => Some(pb::auth_info::Info::Jwt(pb::auth_info::Jwt {})),
            mt::principal::AuthInfo::Password {
                hash_value,
                hash_method,
            } => Some(pb::auth_info::Info::Password(pb::auth_info::Password {
                hash_value: hash_value.clone(),
                hash_method: *hash_method as i32,
            })),
        };
        Ok(pb::AuthInfo {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            info,
        })
    }
}

impl FromToProto for mt::principal::UserOption {
    type PB = pb::UserOption;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: pb::UserOption) -> Result<Self, Incompatible>
    where Self: Sized {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        // ignore unknown flags
        let flags = BitFlags::<mt::principal::UserOptionFlag, u64>::from_bits_truncate(p.flags);

        Ok(mt::principal::UserOption::default()
            .with_flags(flags)
            .with_default_role(p.default_role))
    }

    fn to_pb(&self) -> Result<pb::UserOption, Incompatible> {
        Ok(pb::UserOption {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            flags: self.flags().bits(),
            default_role: self.default_role().cloned(),
        })
    }
}

impl FromToProto for mt::principal::UserQuota {
    type PB = pb::UserQuota;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: pb::UserQuota) -> Result<Self, Incompatible>
    where Self: Sized {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        Ok(Self {
            max_cpu: p.max_cpu,
            max_memory_in_bytes: p.max_memory_in_bytes,
            max_storage_in_bytes: p.max_storage_in_bytes,
        })
    }

    fn to_pb(&self) -> Result<pb::UserQuota, Incompatible> {
        Ok(pb::UserQuota {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            max_cpu: self.max_cpu,
            max_memory_in_bytes: self.max_memory_in_bytes,
            max_storage_in_bytes: self.max_storage_in_bytes,
        })
    }
}

impl FromToProto for mt::principal::GrantObject {
    type PB = pb::GrantObject;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: pb::GrantObject) -> Result<Self, Incompatible>
    where Self: Sized {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        match p.object {
            Some(pb::grant_object::Object::Global(pb::grant_object::GrantGlobalObject {})) => {
                Ok(mt::principal::GrantObject::Global)
            }
            Some(pb::grant_object::Object::Database(pb::grant_object::GrantDatabaseObject {
                catalog,
                db,
            })) => Ok(mt::principal::GrantObject::Database(catalog, db)),
            Some(pb::grant_object::Object::Table(pb::grant_object::GrantTableObject {
                catalog,
                db,
                table,
            })) => Ok(mt::principal::GrantObject::Table(catalog, db, table)),
            _ => Err(Incompatible {
                reason: "GrantObject cannot be None".to_string(),
            }),
        }
    }

    fn to_pb(&self) -> Result<pb::GrantObject, Incompatible> {
        let object = match self {
            mt::principal::GrantObject::Global => Some(pb::grant_object::Object::Global(
                pb::grant_object::GrantGlobalObject {},
            )),
            mt::principal::GrantObject::Database(catalog, db) => Some(
                pb::grant_object::Object::Database(pb::grant_object::GrantDatabaseObject {
                    catalog: catalog.clone(),
                    db: db.clone(),
                }),
            ),
            mt::principal::GrantObject::Table(catalog, db, table) => Some(
                pb::grant_object::Object::Table(pb::grant_object::GrantTableObject {
                    catalog: catalog.clone(),
                    db: db.clone(),
                    table: table.clone(),
                }),
            ),
        };
        Ok(pb::GrantObject {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            object,
        })
    }
}

impl FromToProto for mt::principal::GrantEntry {
    type PB = pb::GrantEntry;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: pb::GrantEntry) -> Result<Self, Incompatible>
    where Self: Sized {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        let privileges = BitFlags::<mt::principal::UserPrivilegeType, u64>::from_bits(p.privileges);
        match privileges {
            Ok(privileges) => Ok(mt::principal::GrantEntry::new(
                mt::principal::GrantObject::from_pb(p.object.ok_or_else(|| Incompatible {
                    reason: "GrantEntry.object can not be None".to_string(),
                })?)?,
                privileges,
            )),
            Err(e) => Err(Incompatible {
                reason: format!("UserPrivilegeType error: {}", e),
            }),
        }
    }

    fn to_pb(&self) -> Result<pb::GrantEntry, Incompatible> {
        Ok(pb::GrantEntry {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            object: Some(self.object().to_pb()?),
            privileges: self.privileges().bits(),
        })
    }
}

impl FromToProto for mt::principal::UserGrantSet {
    type PB = pb::UserGrantSet;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: pb::UserGrantSet) -> Result<Self, Incompatible>
    where Self: Sized {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        let mut entries = Vec::new();
        for entry in p.entries.iter() {
            entries.push(mt::principal::GrantEntry::from_pb(entry.clone())?);
        }
        let mut roles = HashSet::new();
        for role in p.roles.iter() {
            roles.insert(role.0.clone());
        }
        Ok(mt::principal::UserGrantSet::new(entries, roles))
    }

    fn to_pb(&self) -> Result<pb::UserGrantSet, Incompatible> {
        let mut entries = Vec::new();
        for entry in self.entries().iter() {
            entries.push(entry.to_pb()?);
        }

        let mut roles = BTreeMap::new();
        for role in self.roles().iter() {
            roles.insert(role.clone(), true);
        }

        Ok(pb::UserGrantSet {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            entries,
            roles,
        })
    }
}

impl FromToProto for mt::principal::UserInfo {
    type PB = pb::UserInfo;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: pb::UserInfo) -> Result<Self, Incompatible>
    where Self: Sized {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        Ok(mt::principal::UserInfo {
            name: p.name.clone(),
            hostname: p.hostname.clone(),
            auth_info: mt::principal::AuthInfo::from_pb(p.auth_info.ok_or_else(|| {
                Incompatible {
                    reason: "UserInfo.auth_info cannot be None".to_string(),
                }
            })?)?,
            grants: mt::principal::UserGrantSet::from_pb(p.grants.ok_or_else(|| {
                Incompatible {
                    reason: "UserInfo.grants cannot be None".to_string(),
                }
            })?)?,
            quota: mt::principal::UserQuota::from_pb(p.quota.ok_or_else(|| Incompatible {
                reason: "UserInfo.quota cannot be None".to_string(),
            })?)?,
            option: mt::principal::UserOption::from_pb(p.option.ok_or_else(|| Incompatible {
                reason: "UserInfo.option cannot be None".to_string(),
            })?)?,
        })
    }

    fn to_pb(&self) -> Result<pb::UserInfo, Incompatible> {
        Ok(pb::UserInfo {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            name: self.name.clone(),
            hostname: self.hostname.clone(),
            auth_info: Some(mt::principal::AuthInfo::to_pb(&self.auth_info)?),
            grants: Some(mt::principal::UserGrantSet::to_pb(&self.grants)?),
            quota: Some(mt::principal::UserQuota::to_pb(&self.quota)?),
            option: Some(mt::principal::UserOption::to_pb(&self.option)?),
        })
    }
}

impl FromToProto for mt::principal::UserIdentity {
    type PB = pb::UserIdentity;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: pb::UserIdentity) -> Result<Self, Incompatible>
    where Self: Sized {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        Ok(mt::principal::UserIdentity {
            username: p.username.clone(),
            hostname: p.hostname,
        })
    }

    fn to_pb(&self) -> Result<pb::UserIdentity, Incompatible> {
        Ok(pb::UserIdentity {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            username: self.username.clone(),
            hostname: self.hostname.clone(),
        })
    }
}

impl FromToProto for mt::principal::StageFileFormatType {
    type PB = pb::stage_info::StageFileFormatType;
    fn get_pb_ver(_p: &Self::PB) -> u64 {
        0
    }
    fn from_pb(p: pb::stage_info::StageFileFormatType) -> Result<Self, Incompatible>
    where Self: Sized {
        match p {
            pb::stage_info::StageFileFormatType::Csv => Ok(mt::principal::StageFileFormatType::Csv),
            pb::stage_info::StageFileFormatType::Tsv => Ok(mt::principal::StageFileFormatType::Tsv),
            pb::stage_info::StageFileFormatType::Json => {
                Ok(mt::principal::StageFileFormatType::Json)
            }
            pb::stage_info::StageFileFormatType::NdJson => {
                Ok(mt::principal::StageFileFormatType::NdJson)
            }
            pb::stage_info::StageFileFormatType::Avro => {
                Ok(mt::principal::StageFileFormatType::Avro)
            }
            pb::stage_info::StageFileFormatType::Orc => Ok(mt::principal::StageFileFormatType::Orc),
            pb::stage_info::StageFileFormatType::Parquet => {
                Ok(mt::principal::StageFileFormatType::Parquet)
            }
            pb::stage_info::StageFileFormatType::Xml => Ok(mt::principal::StageFileFormatType::Xml),
        }
    }

    fn to_pb(&self) -> Result<pb::stage_info::StageFileFormatType, Incompatible> {
        match *self {
            mt::principal::StageFileFormatType::Csv => Ok(pb::stage_info::StageFileFormatType::Csv),
            mt::principal::StageFileFormatType::Tsv => Ok(pb::stage_info::StageFileFormatType::Tsv),
            mt::principal::StageFileFormatType::Json => {
                Ok(pb::stage_info::StageFileFormatType::Json)
            }
            mt::principal::StageFileFormatType::NdJson => {
                Ok(pb::stage_info::StageFileFormatType::NdJson)
            }
            mt::principal::StageFileFormatType::Avro => {
                Ok(pb::stage_info::StageFileFormatType::Avro)
            }
            mt::principal::StageFileFormatType::Orc => Ok(pb::stage_info::StageFileFormatType::Orc),
            mt::principal::StageFileFormatType::Parquet => {
                Ok(pb::stage_info::StageFileFormatType::Parquet)
            }
            mt::principal::StageFileFormatType::Xml => Ok(pb::stage_info::StageFileFormatType::Xml),
            mt::principal::StageFileFormatType::None => Err(Incompatible {
                reason: "StageFileFormatType::None cannot be converted to protobuf".to_string(),
            }),
        }
    }
}

impl FromToProto for mt::principal::StageFileCompression {
    type PB = pb::stage_info::StageFileCompression;
    fn get_pb_ver(_p: &Self::PB) -> u64 {
        0
    }
    fn from_pb(p: pb::stage_info::StageFileCompression) -> Result<Self, Incompatible>
    where Self: Sized {
        match p {
            pb::stage_info::StageFileCompression::Auto => {
                Ok(mt::principal::StageFileCompression::Auto)
            }
            pb::stage_info::StageFileCompression::Gzip => {
                Ok(mt::principal::StageFileCompression::Gzip)
            }
            pb::stage_info::StageFileCompression::Bz2 => {
                Ok(mt::principal::StageFileCompression::Bz2)
            }
            pb::stage_info::StageFileCompression::Brotli => {
                Ok(mt::principal::StageFileCompression::Brotli)
            }
            pb::stage_info::StageFileCompression::Zstd => {
                Ok(mt::principal::StageFileCompression::Zstd)
            }
            pb::stage_info::StageFileCompression::Deflate => {
                Ok(mt::principal::StageFileCompression::Deflate)
            }
            pb::stage_info::StageFileCompression::RawDeflate => {
                Ok(mt::principal::StageFileCompression::RawDeflate)
            }
            pb::stage_info::StageFileCompression::Lzo => {
                Ok(mt::principal::StageFileCompression::Lzo)
            }
            pb::stage_info::StageFileCompression::Snappy => {
                Ok(mt::principal::StageFileCompression::Snappy)
            }
            pb::stage_info::StageFileCompression::None => {
                Ok(mt::principal::StageFileCompression::None)
            }
            pb::stage_info::StageFileCompression::Xz => Ok(mt::principal::StageFileCompression::Xz),
        }
    }

    fn to_pb(&self) -> Result<pb::stage_info::StageFileCompression, Incompatible> {
        match *self {
            mt::principal::StageFileCompression::Auto => {
                Ok(pb::stage_info::StageFileCompression::Auto)
            }
            mt::principal::StageFileCompression::Gzip => {
                Ok(pb::stage_info::StageFileCompression::Gzip)
            }
            mt::principal::StageFileCompression::Bz2 => {
                Ok(pb::stage_info::StageFileCompression::Bz2)
            }
            mt::principal::StageFileCompression::Brotli => {
                Ok(pb::stage_info::StageFileCompression::Brotli)
            }
            mt::principal::StageFileCompression::Zstd => {
                Ok(pb::stage_info::StageFileCompression::Zstd)
            }
            mt::principal::StageFileCompression::Deflate => {
                Ok(pb::stage_info::StageFileCompression::Deflate)
            }
            mt::principal::StageFileCompression::RawDeflate => {
                Ok(pb::stage_info::StageFileCompression::RawDeflate)
            }
            mt::principal::StageFileCompression::Lzo => {
                Ok(pb::stage_info::StageFileCompression::Lzo)
            }
            mt::principal::StageFileCompression::Snappy => {
                Ok(pb::stage_info::StageFileCompression::Snappy)
            }
            mt::principal::StageFileCompression::None => {
                Ok(pb::stage_info::StageFileCompression::None)
            }
            mt::principal::StageFileCompression::Xz => Ok(pb::stage_info::StageFileCompression::Xz),
        }
    }
}

impl FromToProto for mt::principal::StageType {
    type PB = pb::stage_info::StageType;
    fn get_pb_ver(_p: &Self::PB) -> u64 {
        0
    }
    fn from_pb(p: pb::stage_info::StageType) -> Result<Self, Incompatible>
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

    fn to_pb(&self) -> Result<pb::stage_info::StageType, Incompatible> {
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

impl FromToProto for mt::storage::StorageParams {
    type PB = pb::stage_info::StageStorage;
    fn get_pb_ver(_p: &Self::PB) -> u64 {
        0
    }
    fn from_pb(p: pb::stage_info::StageStorage) -> Result<Self, Incompatible>
    where Self: Sized {
        match p.storage {
            Some(pb::stage_info::stage_storage::Storage::S3(s)) => Ok(
                mt::storage::StorageParams::S3(mt::storage::StorageS3Config::from_pb(s)?),
            ),
            Some(pb::stage_info::stage_storage::Storage::Fs(s)) => Ok(
                mt::storage::StorageParams::Fs(mt::storage::StorageFsConfig::from_pb(s)?),
            ),
            Some(pb::stage_info::stage_storage::Storage::Gcs(s)) => Ok(
                mt::storage::StorageParams::Gcs(mt::storage::StorageGcsConfig::from_pb(s)?),
            ),
            Some(pb::stage_info::stage_storage::Storage::Oss(s)) => Ok(
                mt::storage::StorageParams::Oss(mt::storage::StorageOssConfig::from_pb(s)?),
            ),
            Some(pb::stage_info::stage_storage::Storage::Webhdfs(s)) => Ok(
                mt::storage::StorageParams::Webhdfs(mt::storage::StorageWebhdfsConfig::from_pb(s)?),
            ),
            None => Err(Incompatible {
                reason: "StageStorage.storage cannot be None".to_string(),
            }),
        }
    }

    fn to_pb(&self) -> Result<pb::stage_info::StageStorage, Incompatible> {
        match self {
            mt::storage::StorageParams::S3(v) => Ok(pb::stage_info::StageStorage {
                storage: Some(pb::stage_info::stage_storage::Storage::S3(v.to_pb()?)),
            }),
            mt::storage::StorageParams::Fs(v) => Ok(pb::stage_info::StageStorage {
                storage: Some(pb::stage_info::stage_storage::Storage::Fs(v.to_pb()?)),
            }),
            mt::storage::StorageParams::Gcs(v) => Ok(pb::stage_info::StageStorage {
                storage: Some(pb::stage_info::stage_storage::Storage::Gcs(v.to_pb()?)),
            }),
            mt::storage::StorageParams::Oss(v) => Ok(pb::stage_info::StageStorage {
                storage: Some(pb::stage_info::stage_storage::Storage::Oss(v.to_pb()?)),
            }),
            mt::storage::StorageParams::Webhdfs(v) => Ok(pb::stage_info::StageStorage {
                storage: Some(pb::stage_info::stage_storage::Storage::Webhdfs(v.to_pb()?)),
            }),
            others => Err(Incompatible {
                reason: format!("stage type: {} not supported", others),
            }),
        }
    }
}

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
    type PB = pb::stage_info::FileFormatOptions;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: pb::stage_info::FileFormatOptions) -> Result<Self, Incompatible>
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

    fn to_pb(&self) -> Result<pb::stage_info::FileFormatOptions, Incompatible> {
        let format = mt::principal::StageFileFormatType::to_pb(&self.format)? as i32;
        let compression = mt::principal::StageFileCompression::to_pb(&self.compression)? as i32;
        Ok(pb::stage_info::FileFormatOptions {
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
    type PB = pb::stage_info::UserDefinedFileFormat;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: pb::stage_info::UserDefinedFileFormat) -> Result<Self, Incompatible>
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

    fn to_pb(&self) -> Result<pb::stage_info::UserDefinedFileFormat, Incompatible> {
        let file_format_options =
            mt::principal::FileFormatOptions::to_pb(&self.file_format_options)?;
        let creator = mt::principal::UserIdentity::to_pb(&self.creator)?;
        Ok(pb::stage_info::UserDefinedFileFormat {
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
