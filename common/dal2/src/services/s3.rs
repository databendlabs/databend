use async_compat::CompatExt;
use async_trait::async_trait;
use azure_storage_mirror::MetadataDetail::Default;
use common_exception::Result;
use futures;
use rusoto_core::Region;
use rusoto_s3::GetObjectRequest;
use rusoto_s3::S3Client;
use rusoto_s3::S3 as RusotoS3;
use tokio;
use tokio::io::AsyncSeek;
use tokio::io::AsyncSeekExt;

use crate::ops::Read;
use crate::ops::ReadBuilder;
use crate::ops::Reader;

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
            bucket: self.bucket.clone(),
        }
    }
}

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
        let mut req = GetObjectRequest {
            bucket: self.bucket.clone(),
            key: args.path.to_string(),
            ..GetObjectRequest::default()
        };

        // TODO: Handle range header here.

        let mut resp = self.client.get_object(req).await.unwrap();

        if resp.body.is_none() {
            panic!("Body is empty")
        }

        Ok(Box::new(resp.body.unwrap().into_async_read().compat()))
    }
}
