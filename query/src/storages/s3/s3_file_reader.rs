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
use opendal::credential::Credential;
use opendal::Reader;

pub struct S3FileReader {}

impl S3FileReader {
    pub async fn create(
        file_name: Option<String>,
        s3_endpoint: &str,
        s3_bucket: &str,
        s3_path: &str,
        aws_key_id: &str,
        aws_secret_key: &str,
    ) -> Result<Reader> {
        let mut builder = opendal::services::s3::Backend::build();

        // Endpoint url.
        if !s3_endpoint.is_empty() {
            builder.endpoint(s3_endpoint);
        }

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
            .map_err(|e| ErrorCode::DalS3Error(format!("s3 dal build error:{:?}", e)))?;
        let operator = opendal::Operator::new(accessor);

        let path = match file_name {
            None => s3_path.to_string(),
            Some(v) => {
                let mut path = s3_path.to_string();
                if path.starts_with('/') {
                    path.remove(0);
                }
                if path.ends_with('/') {
                    path.pop();
                }
                format!("{}/{}", path, v)
            }
        };
        Ok(operator.object(&path).reader())
    }
}
