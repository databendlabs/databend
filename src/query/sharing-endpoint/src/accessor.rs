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

use common_base::base::GlobalInstance;
use common_exception::Result;
use common_storage::init_operator;
use common_storage::StorageParams;
use opendal::Operator;
use time::Duration;

use crate::configs::Config;
use crate::models;
use crate::models::PresignFileResponse;
use crate::models::SharedTableResponse;

#[derive(Clone)]
pub struct SharingAccessor {
    op: Operator,
    config: Config,
}

// file would have two kind of path:
// 1. with root. e.g. /root1/root2/root3/db1/tb1/file1
// 2. without root e.g. db1/tb1/file1
// after it would be converted to file1
// and then it would use the location in table spec to form the final path
// {localtion}/file1
pub fn truncate_root(root: String, loc: String) -> String {
    let root = root.trim_matches('/');
    let loc = loc.trim_matches('/');
    return if loc.starts_with(root) {
        let o1 = loc.strip_prefix(root).unwrap();

        let updated = o1.trim_matches('/');
        let arr = updated.split('/').collect::<Vec<&str>>();
        if arr.len() > 2 {
            return arr[2..].join("/");
        }
        updated.to_string()
    } else {
        let arr = loc.split('/').collect::<Vec<&str>>();
        if arr.len() > 2 {
            return arr[2..].join("/");
        }
        loc.to_string()
    };
}

impl SharingAccessor {
    pub async fn init(cfg: &Config) -> Result<()> {
        GlobalInstance::set(Self::try_create(cfg).await?);

        Ok(())
    }

    pub fn instance() -> SharingAccessor {
        GlobalInstance::get()
    }

    pub async fn try_create(cfg: &Config) -> Result<SharingAccessor> {
        let operator = init_operator(&cfg.storage.params)?;

        Ok(SharingAccessor {
            op: operator,
            config: cfg.clone(),
        })
    }
    fn get_root(&self) -> String {
        match self.config.storage.params {
            StorageParams::S3(ref s3) => s3.root.trim_matches('/').to_string(),
            StorageParams::Oss(ref oss) => oss.root.trim_matches('/').to_string(),
            _ => "".to_string(),
        }
    }

    fn get_share_location(&self) -> String {
        format!("{}/_share_config/share_specs.json", self.config.tenant)
    }

    // read share table spec from S3 and check whether requester has permission on the table
    pub async fn get_shared_table(
        &self,
        input: &models::LambdaInput,
    ) -> Result<Option<SharedTableResponse>> {
        let sharing_accessor = Self::instance();
        let path = sharing_accessor.get_share_location();
        let data = sharing_accessor.op.object(&path).read().await?;
        let share_specs: models::SharingConfig = serde_json::from_slice(data.as_slice())?;
        share_specs.get_tables(input)
    }

    // presign_file would be separated into two steps:
    // 1. fetch the table location
    // 2. form the final path and presign it
    pub async fn presign_file(
        &self,
        table: &SharedTableResponse,
        input: &models::RequestFile,
    ) -> Result<PresignFileResponse> {
        let loc_prefix = table.location.trim_matches('/');
        let loc_prefix = loc_prefix.strip_prefix(self.get_root().as_str()).unwrap();

        let file_path = truncate_root(self.get_root(), input.file_name.clone());
        let obj_path = format!("{}/{}", loc_prefix, file_path);
        let op = self.op.clone();
        if input.method == "HEAD" {
            let s = op
                .object(obj_path.as_str())
                .presign_stat(Duration::hours(1))?;
            return Ok(PresignFileResponse::new(&s, input.file_name.clone()));
        }

        let s = op
            .object(obj_path.as_str())
            .presign_read(Duration::hours(1))?;
        Ok(PresignFileResponse::new(&s, input.file_name.clone()))
    }

    pub async fn get_presigned_files(
        input: &models::LambdaInput,
    ) -> Result<Vec<PresignFileResponse>> {
        let accessor = Self::instance();
        let table = accessor.get_shared_table(input).await?;
        return match table {
            Some(t) => {
                let mut presigned_files = vec![];
                for f in input.request_files.iter() {
                    let presigned_file = accessor.presign_file(&t, f).await?;
                    presigned_files.push(presigned_file);
                }
                Ok(presigned_files)
            }
            None => Ok(vec![]),
        };
    }
}
