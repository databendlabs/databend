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

use common_io::prelude::StorageFsConfig;
use common_io::prelude::StorageParams;
use common_io::prelude::StorageS3Config;
use common_protos::pb;

use crate::check_ver;
use crate::FromToProto;
use crate::Incompatible;
use crate::MIN_COMPATIBLE_VER;
use crate::VER;

impl FromToProto<pb::S3StorageConfig> for StorageParams {
    fn from_pb(p: pb::S3StorageConfig) -> Result<Self, Incompatible>
    where Self: Sized {
        // TODO: config will have it's own version flags in the future.
        check_ver(p.version, p.min_compatible)?;

        Ok(Self::S3(StorageS3Config {
            region: p.region,
            endpoint_url: p.endpoint_url,
            access_key_id: p.access_key_id,
            secret_access_key: p.secret_access_key,
            bucket: p.bucket,
            root: p.root,
            master_key: p.master_key,
            disable_credential_loader: p.disable_credential_loader,
            enable_virtual_host_style: p.enable_virtual_host_style,
        }))
    }

    fn to_pb(&self) -> Result<pb::S3StorageConfig, Incompatible> {
        if let StorageParams::S3(v) = self {
            Ok(pb::S3StorageConfig {
                version: VER,
                min_compatible: MIN_COMPATIBLE_VER,
                region: v.region.clone(),
                endpoint_url: v.endpoint_url.clone(),
                access_key_id: v.access_key_id.clone(),
                secret_access_key: v.secret_access_key.clone(),
                bucket: v.bucket.clone(),
                root: v.root.clone(),
                master_key: v.master_key.clone(),
                disable_credential_loader: v.disable_credential_loader,
                enable_virtual_host_style: v.enable_virtual_host_style,
            })
        } else {
            Err(Incompatible {
                reason: "storage type mismatch".to_string(),
            })
        }
    }
}

impl FromToProto<pb::FsStorageConfig> for StorageParams {
    fn from_pb(p: pb::FsStorageConfig) -> Result<Self, Incompatible>
    where Self: Sized {
        // TODO: config will have it's own version flags in the future.
        check_ver(p.version, p.min_compatible)?;

        Ok(Self::Fs(StorageFsConfig { root: p.root }))
    }

    fn to_pb(&self) -> Result<pb::FsStorageConfig, Incompatible> {
        if let StorageParams::Fs(v) = self {
            Ok(pb::FsStorageConfig {
                version: VER,
                min_compatible: MIN_COMPATIBLE_VER,
                root: v.root.clone(),
            })
        } else {
            Err(Incompatible {
                reason: "storage type mismatch".to_string(),
            })
        }
    }
}
