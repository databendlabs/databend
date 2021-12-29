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

use async_compat::CompatExt;
use async_trait::async_trait;
use common_exception::Result;
use rusoto_core::Region;
use rusoto_s3::GetObjectRequest;
use rusoto_s3::S3Client;
use rusoto_s3::S3 as RusotoS3;

use crate::ops::Read;
use crate::ops::ReadBuilder;
use crate::ops::Reader;

/// TODO: https://github.com/datafuselabs/databend/issues/3677
#[derive(Default)]
pub struct Builder {
    pub bucket: String,
    pub endpoint: String,
    pub credential: String,
}

impl Builder {
    pub fn finish(self) -> Backend {
        Backend {
            client: S3Client::new(Region::default()),
            bucket: self.bucket,
        }
    }
}

/// TODO: https://github.com/datafuselabs/databend/issues/3677
pub struct Backend {
    client: S3Client,
    bucket: String,
}

impl Backend {
    pub fn build() -> Builder {
        Builder::default()
    }
}

#[async_trait]
impl<S: Send + Sync> Read<S> for Backend {
    async fn read(&self, args: &ReadBuilder<S>) -> Result<Reader> {
        let req = GetObjectRequest {
            bucket: self.bucket.clone(),
            key: args.path.to_string(),
            ..GetObjectRequest::default()
        };

        // TODO: Handle range header here.

        let resp = self.client.get_object(req).await.unwrap();

        if resp.body.is_none() {
            panic!("Body is empty")
        }

        Ok(Box::new(resp.body.unwrap().into_async_read().compat()))
    }
}
