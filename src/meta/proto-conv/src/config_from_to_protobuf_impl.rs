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

use databend_common_meta_app as mt;
use databend_common_meta_app::storage::StorageCosConfig;
use databend_common_meta_app::storage::StorageFsConfig;
use databend_common_meta_app::storage::StorageGcsConfig;
use databend_common_meta_app::storage::StorageHdfsConfig;
use databend_common_meta_app::storage::StorageObsConfig;
use databend_common_meta_app::storage::StorageOssConfig;
use databend_common_meta_app::storage::StorageS3Config;
use databend_common_meta_app::storage::StorageWebhdfsConfig;
use databend_common_protos::pb;

use crate::reader_check_msg;
use crate::FromToProto;
use crate::Incompatible;
use crate::MIN_READER_VER;
use crate::VER;

impl FromToProto for mt::storage::StorageParams {
    type PB = pb::StorageConfig;
    fn get_pb_ver(_p: &Self::PB) -> u64 {
        0
    }
    fn from_pb(p: pb::StorageConfig) -> Result<Self, Incompatible>
    where Self: Sized {
        match p.storage {
            Some(pb::storage_config::Storage::S3(s)) => Ok(mt::storage::StorageParams::S3(
                mt::storage::StorageS3Config::from_pb(s)?,
            )),
            Some(pb::storage_config::Storage::Fs(s)) => Ok(mt::storage::StorageParams::Fs(
                mt::storage::StorageFsConfig::from_pb(s)?,
            )),
            Some(pb::storage_config::Storage::Gcs(s)) => Ok(mt::storage::StorageParams::Gcs(
                mt::storage::StorageGcsConfig::from_pb(s)?,
            )),
            Some(pb::storage_config::Storage::Oss(s)) => Ok(mt::storage::StorageParams::Oss(
                mt::storage::StorageOssConfig::from_pb(s)?,
            )),
            Some(pb::storage_config::Storage::Webhdfs(s)) => Ok(
                mt::storage::StorageParams::Webhdfs(mt::storage::StorageWebhdfsConfig::from_pb(s)?),
            ),
            Some(pb::storage_config::Storage::Obs(s)) => Ok(mt::storage::StorageParams::Obs(
                mt::storage::StorageObsConfig::from_pb(s)?,
            )),
            Some(pb::storage_config::Storage::Cos(s)) => Ok(mt::storage::StorageParams::Cos(
                mt::storage::StorageCosConfig::from_pb(s)?,
            )),
            Some(pb::storage_config::Storage::Hdfs(s)) => Ok(mt::storage::StorageParams::Hdfs(
                mt::storage::StorageHdfsConfig::from_pb(s)?,
            )),
            Some(pb::storage_config::Storage::Huggingface(s)) => {
                Ok(mt::storage::StorageParams::Huggingface(
                    mt::storage::StorageHuggingfaceConfig::from_pb(s)?,
                ))
            }
            Some(pb::storage_config::Storage::Azblob(s)) => Ok(mt::storage::StorageParams::Azblob(
                mt::storage::StorageAzblobConfig::from_pb(s)?,
            )),
            None => Err(Incompatible {
                reason: "StageStorage.storage cannot be None".to_string(),
            }),
        }
    }

    fn to_pb(&self) -> Result<pb::StorageConfig, Incompatible> {
        match self {
            mt::storage::StorageParams::S3(v) => Ok(pb::StorageConfig {
                storage: Some(pb::storage_config::Storage::S3(v.to_pb()?)),
            }),
            mt::storage::StorageParams::Fs(v) => Ok(pb::StorageConfig {
                storage: Some(pb::storage_config::Storage::Fs(v.to_pb()?)),
            }),
            mt::storage::StorageParams::Gcs(v) => Ok(pb::StorageConfig {
                storage: Some(pb::storage_config::Storage::Gcs(v.to_pb()?)),
            }),
            mt::storage::StorageParams::Oss(v) => Ok(pb::StorageConfig {
                storage: Some(pb::storage_config::Storage::Oss(v.to_pb()?)),
            }),
            mt::storage::StorageParams::Webhdfs(v) => Ok(pb::StorageConfig {
                storage: Some(pb::storage_config::Storage::Webhdfs(v.to_pb()?)),
            }),
            mt::storage::StorageParams::Obs(v) => Ok(pb::StorageConfig {
                storage: Some(pb::storage_config::Storage::Obs(v.to_pb()?)),
            }),
            mt::storage::StorageParams::Cos(v) => Ok(pb::StorageConfig {
                storage: Some(pb::storage_config::Storage::Cos(v.to_pb()?)),
            }),
            mt::storage::StorageParams::Hdfs(v) => Ok(pb::StorageConfig {
                storage: Some(pb::storage_config::Storage::Hdfs(v.to_pb()?)),
            }),
            mt::storage::StorageParams::Huggingface(v) => Ok(pb::StorageConfig {
                storage: Some(pb::storage_config::Storage::Huggingface(v.to_pb()?)),
            }),
            mt::storage::StorageParams::Azblob(v) => Ok(pb::StorageConfig {
                storage: Some(pb::storage_config::Storage::Azblob(v.to_pb()?)),
            }),
            others => Err(Incompatible {
                reason: format!("stage type: {} not supported", others),
            }),
        }
    }
}

impl FromToProto for StorageS3Config {
    type PB = pb::S3StorageConfig;

    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.version
    }

    fn from_pb(p: pb::S3StorageConfig) -> Result<Self, Incompatible>
    where Self: Sized {
        reader_check_msg(p.version, p.min_reader_ver)?;

        Ok(StorageS3Config {
            region: p.region,
            endpoint_url: p.endpoint_url,
            access_key_id: p.access_key_id,
            secret_access_key: p.secret_access_key,
            security_token: p.security_token,
            bucket: p.bucket,
            root: p.root,
            master_key: p.master_key,
            disable_credential_loader: p.disable_credential_loader,
            enable_virtual_host_style: p.enable_virtual_host_style,
            role_arn: p.role_arn,
            external_id: p.external_id,
        })
    }

    fn to_pb(&self) -> Result<pb::S3StorageConfig, Incompatible> {
        Ok(pb::S3StorageConfig {
            version: VER,
            min_reader_ver: MIN_READER_VER,
            region: self.region.clone(),
            endpoint_url: self.endpoint_url.clone(),
            access_key_id: self.access_key_id.clone(),
            secret_access_key: self.secret_access_key.clone(),
            security_token: self.security_token.clone(),
            bucket: self.bucket.clone(),
            root: self.root.clone(),
            master_key: self.master_key.clone(),
            disable_credential_loader: self.disable_credential_loader,
            enable_virtual_host_style: self.enable_virtual_host_style,
            role_arn: self.role_arn.clone(),
            external_id: self.external_id.clone(),
        })
    }
}

impl FromToProto for StorageGcsConfig {
    type PB = pb::GcsStorageConfig;

    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.version
    }

    fn from_pb(p: Self::PB) -> Result<Self, Incompatible>
    where Self: Sized {
        reader_check_msg(p.version, p.min_reader_ver)?;

        Ok(StorageGcsConfig {
            credential: p.credential,
            endpoint_url: p.endpoint_url,
            bucket: p.bucket,
            root: p.root,
        })
    }

    fn to_pb(&self) -> Result<Self::PB, Incompatible> {
        Ok(pb::GcsStorageConfig {
            version: VER,
            min_reader_ver: MIN_READER_VER,
            credential: self.credential.clone(),
            endpoint_url: self.endpoint_url.clone(),
            bucket: self.bucket.clone(),
            root: self.root.clone(),
        })
    }
}

impl FromToProto for StorageFsConfig {
    type PB = pb::FsStorageConfig;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.version
    }

    fn from_pb(p: pb::FsStorageConfig) -> Result<Self, Incompatible>
    where Self: Sized {
        reader_check_msg(p.version, p.min_reader_ver)?;

        Ok(StorageFsConfig { root: p.root })
    }

    fn to_pb(&self) -> Result<pb::FsStorageConfig, Incompatible> {
        Ok(pb::FsStorageConfig {
            version: VER,
            min_reader_ver: MIN_READER_VER,
            root: self.root.clone(),
        })
    }
}

impl FromToProto for StorageOssConfig {
    type PB = pb::OssStorageConfig;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.version
    }

    fn from_pb(p: pb::OssStorageConfig) -> Result<Self, Incompatible>
    where Self: Sized {
        reader_check_msg(p.version, p.min_reader_ver)?;

        Ok(StorageOssConfig {
            endpoint_url: p.endpoint_url,
            presign_endpoint_url: "".to_string(),
            bucket: p.bucket,
            root: p.root,

            access_key_id: p.access_key_id,
            access_key_secret: p.access_key_secret,
            server_side_encryption: p.server_side_encryption,
            server_side_encryption_key_id: p.server_side_encryption_key_id,
        })
    }

    fn to_pb(&self) -> Result<pb::OssStorageConfig, Incompatible> {
        Ok(pb::OssStorageConfig {
            version: VER,
            min_reader_ver: MIN_READER_VER,
            endpoint_url: self.endpoint_url.clone(),
            bucket: self.bucket.clone(),
            root: self.root.clone(),
            access_key_id: self.access_key_id.clone(),
            access_key_secret: self.access_key_secret.clone(),
            server_side_encryption: self.server_side_encryption.clone(),
            server_side_encryption_key_id: self.server_side_encryption_key_id.clone(),
        })
    }
}

impl FromToProto for StorageWebhdfsConfig {
    type PB = pb::WebhdfsStorageConfig;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.version
    }

    fn from_pb(p: Self::PB) -> Result<Self, Incompatible>
    where Self: Sized {
        reader_check_msg(p.version, p.min_reader_ver)?;

        Ok(StorageWebhdfsConfig {
            endpoint_url: p.endpoint_url,
            root: p.root,
            delegation: p.delegation,
        })
    }

    fn to_pb(&self) -> Result<pb::WebhdfsStorageConfig, Incompatible> {
        Ok(pb::WebhdfsStorageConfig {
            version: VER,
            min_reader_ver: MIN_READER_VER,
            endpoint_url: self.endpoint_url.clone(),
            root: self.root.clone(),
            delegation: self.delegation.clone(),

            username: String::new(), // reserved for future use
            password: String::new(), // reserved for future use
        })
    }
}

impl FromToProto for StorageObsConfig {
    type PB = pb::ObsStorageConfig;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.version
    }

    fn from_pb(p: pb::ObsStorageConfig) -> Result<Self, Incompatible>
    where Self: Sized {
        reader_check_msg(p.version, p.min_reader_ver)?;

        Ok(StorageObsConfig {
            endpoint_url: p.endpoint_url,
            bucket: p.bucket,
            root: p.root,

            access_key_id: p.access_key_id,
            secret_access_key: p.secret_access_key,
        })
    }

    fn to_pb(&self) -> Result<pb::ObsStorageConfig, Incompatible> {
        Ok(pb::ObsStorageConfig {
            version: VER,
            min_reader_ver: MIN_READER_VER,
            endpoint_url: self.endpoint_url.clone(),
            bucket: self.bucket.clone(),
            root: self.root.clone(),
            access_key_id: self.access_key_id.clone(),
            secret_access_key: self.secret_access_key.clone(),
        })
    }
}

impl FromToProto for StorageCosConfig {
    type PB = pb::CosStorageConfig;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.version
    }

    fn from_pb(p: pb::CosStorageConfig) -> Result<Self, Incompatible>
    where Self: Sized {
        reader_check_msg(p.version, p.min_reader_ver)?;

        Ok(StorageCosConfig {
            endpoint_url: p.endpoint_url,
            bucket: p.bucket,
            root: p.root,

            secret_id: p.secret_id,
            secret_key: p.secret_key,
        })
    }

    fn to_pb(&self) -> Result<pb::CosStorageConfig, Incompatible> {
        Ok(pb::CosStorageConfig {
            version: VER,
            min_reader_ver: MIN_READER_VER,
            endpoint_url: self.endpoint_url.clone(),
            bucket: self.bucket.clone(),
            root: self.root.clone(),
            secret_id: self.secret_id.clone(),
            secret_key: self.secret_key.clone(),
        })
    }
}

impl FromToProto for StorageHdfsConfig {
    type PB = pb::HdfsStorageConfig;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.version
    }

    fn from_pb(p: pb::HdfsStorageConfig) -> Result<Self, Incompatible>
    where Self: Sized {
        reader_check_msg(p.version, p.min_reader_ver)?;

        Ok(StorageHdfsConfig {
            root: p.root,
            name_node: p.name_node,
        })
    }

    fn to_pb(&self) -> Result<pb::HdfsStorageConfig, Incompatible> {
        Ok(pb::HdfsStorageConfig {
            version: VER,
            min_reader_ver: MIN_READER_VER,
            root: self.root.clone(),
            name_node: self.name_node.clone(),
        })
    }
}

impl FromToProto for mt::storage::StorageHuggingfaceConfig {
    type PB = pb::HuggingfaceStorageConfig;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.version
    }

    fn from_pb(p: pb::HuggingfaceStorageConfig) -> Result<Self, Incompatible>
    where Self: Sized {
        reader_check_msg(p.version, p.min_reader_ver)?;

        Ok(mt::storage::StorageHuggingfaceConfig {
            repo_type: p.repo_type,
            repo_id: p.repo_id,
            revision: p.revision,
            token: p.token,
            root: p.root,
        })
    }

    fn to_pb(&self) -> Result<pb::HuggingfaceStorageConfig, Incompatible> {
        Ok(pb::HuggingfaceStorageConfig {
            version: VER,
            min_reader_ver: MIN_READER_VER,
            repo_type: self.repo_type.clone(),
            repo_id: self.repo_id.clone(),
            revision: self.revision.clone(),
            token: self.token.clone(),
            root: self.root.clone(),
        })
    }
}

impl FromToProto for mt::storage::StorageAzblobConfig {
    type PB = pb::AzblobStorageConfig;

    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.version
    }

    fn from_pb(p: Self::PB) -> Result<Self, Incompatible>
    where Self: Sized {
        reader_check_msg(p.version, p.min_reader_ver)?;

        Ok(mt::storage::StorageAzblobConfig {
            endpoint_url: p.endpoint_url,
            container: p.container,
            account_name: p.account_name,
            account_key: p.account_key,
            root: p.root,
        })
    }

    fn to_pb(&self) -> Result<Self::PB, Incompatible> {
        Ok(pb::AzblobStorageConfig {
            version: VER,
            min_reader_ver: MIN_READER_VER,
            endpoint_url: self.endpoint_url.clone(),
            container: self.container.clone(),
            account_name: self.account_name.clone(),
            account_key: self.account_key.clone(),
            root: self.root.clone(),
        })
    }
}
