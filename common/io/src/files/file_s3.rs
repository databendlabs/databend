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

use common_exception::ErrorCode;
use common_exception::Result;
use futures::StreamExt;
use opendal::credential::Credential;
use opendal::ObjectMode;
use opendal::Operator;

pub struct S3File {}

impl S3File {
    // Open a s3 operator.
    pub async fn open(
        s3_endpoint: &str,
        s3_bucket: &str,
        aws_key_id: &str,
        aws_secret_key: &str,
        runtime: &common_base::Runtime,
    ) -> Result<Operator> {
        let mut builder = opendal::services::s3::Backend::build();

        builder.runtime(runtime.inner());

        // Endpoint url.
        builder.endpoint(s3_endpoint);

        // Bucket.
        builder.bucket(s3_bucket);

        // Credentials.
        if !aws_key_id.is_empty() {
            let credential = Credential::hmac(aws_key_id, aws_secret_key);
            builder.credential(credential);
        }

        let accessor = builder
            .finish()
            .await
            .map_err(|e| ErrorCode::DalError(format!("s3 dal build error:{:?}", e)))?;
        Ok(opendal::Operator::new(accessor))
    }

    // Get the files in the path.
    pub async fn list(operator: &Operator, path: &str) -> Result<Vec<String>> {
        let mut list: Vec<String> = vec![];
        // Check the path object mode is DIR or FILE.
        let mode = operator.object(path).metadata().await?.mode();
        match mode {
            ObjectMode::FILE => {
                list.push(path.to_string());
            }
            ObjectMode::DIR => {
                let mut objects = operator.objects(path);
                while let Some(object) = objects.next().await {
                    let mut object = object?;
                    let meta = object.metadata_cached().await?;
                    list.push(meta.path().to_string());
                }
            }
            other => {
                return Err(ErrorCode::DalError(format!(
                    "S3 list() can not handle the object mode: {:?}",
                    other
                )))
            }
        }

        Ok(list)
    }
}
