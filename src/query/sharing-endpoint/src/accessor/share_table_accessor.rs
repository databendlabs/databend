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

use common_exception::Result;
use common_storages_share::SHARE_CONFIG_PREFIX;
use time::Duration;

use crate::accessor::truncate_root;
use crate::accessor::SharingAccessor;
use crate::models;
use crate::models::PresignFileResponse;
use crate::models::SharedTableResponse;

// Methods for access share table spec.
impl SharingAccessor {
    // read share table spec from S3 and check whether requester has permission on the table
    #[async_backtrace::framed]
    async fn get_shared_table_spec(
        &self,
        input: &models::LambdaInput,
    ) -> Result<Option<SharedTableResponse>> {
        let sharing_accessor = Self::instance();
        let path = sharing_accessor.get_share_spec_location();
        let data = sharing_accessor.op.read(&path).await?;
        let share_specs: models::SharingConfig = serde_json::from_slice(data.as_slice())?;
        share_specs.get_tables(input)
    }

    // presign_file would be separated into two steps:
    // 1. fetch the table location
    // 2. form the final path and presign it
    #[async_backtrace::framed]
    async fn share_table_spec_presign_file(
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
            let s = op.presign_stat(obj_path.as_str(), Duration::hours(1))?;
            return Ok(PresignFileResponse::new(&s, input.file_name.clone()));
        }

        let s = op.presign_read(obj_path.as_str(), Duration::hours(1))?;
        Ok(PresignFileResponse::new(&s, input.file_name.clone()))
    }

    pub fn get_share_spec_location(&self) -> String {
        format!(
            "{}/{}/share_specs.json",
            self.config.tenant, SHARE_CONFIG_PREFIX
        )
    }

    #[async_backtrace::framed]
    pub async fn get_share_table_spec_presigned_files(
        input: &models::LambdaInput,
    ) -> Result<Vec<PresignFileResponse>> {
        let accessor = Self::instance();
        let table = accessor.get_shared_table_spec(input).await?;
        match table {
            Some(t) => {
                let mut presigned_files = vec![];
                for f in input.request_files.iter() {
                    let presigned_file = accessor.share_table_spec_presign_file(&t, f).await?;
                    presigned_files.push(presigned_file);
                }
                Ok(presigned_files)
            }
            None => Ok(vec![]),
        }
    }
}
