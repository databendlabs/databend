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

use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_types::StageStorage;
use common_meta_types::UserStageInfo;
use opendal::credential::Credential;
use opendal::Reader;

use crate::sessions::QueryContext;

pub struct DataAccessor {}

impl DataAccessor {
    pub async fn get_source_reader(
        ctx: &Arc<QueryContext>,
        stage_info: &UserStageInfo,
    ) -> Result<Reader> {
        match &stage_info.stage_params.storage {
            StageStorage::S3(s3) => {
                let mut builder = opendal::services::s3::Backend::build();

                // Endpoint url.
                {
                    let endpoint = ctx.get_config().storage.s3.endpoint_url;
                    builder.endpoint(&endpoint);
                }

                // Bucket.
                {
                    let bucket = &s3.bucket;
                    builder.bucket(bucket);
                }

                // Credentials.
                if !s3.credentials_aws_key_id.is_empty() {
                    let key_id = &s3.credentials_aws_key_id;
                    let secret_key = &s3.credentials_aws_secret_key;
                    let credential = Credential::hmac(key_id, secret_key);
                    builder.credential(credential);
                }

                let accessor = builder
                    .finish()
                    .await
                    .map_err(|e| ErrorCode::DalS3Error(format!("s3 dal build error:{:?}", e)))?;
                let operator = opendal::Operator::new(accessor);
                Ok(operator.object(&s3.path).reader())
            }
        }
    }
}
