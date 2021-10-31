//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

use std::sync::Arc;

use azure_core_mirror::HttpClient;
use azure_storage_mirror::clients::StorageAccountClient;
use azure_storage_mirror::core::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use futures::Stream;
use futures::StreamExt;

use crate::AzureBlobInputStream;
use crate::Bytes;
use crate::DataAccessor;
use crate::InputStream;

pub struct AzureBlobAccessor {
    client: Arc<StorageClient>,
    container: String,
}

impl AzureBlobAccessor {
    /// Create a azure blob accessor instance without credentials.
    /// The code will use env variable "STORAGE_ACCOUNT" and "STORAGE_MASTER_KEY".
    #[allow(dead_code)]
    pub fn try_create(account: impl Into<String>, container: impl Into<String>) -> Result<Self> {
        let master_key_res = std::env::var("STORAGE_MASTER_KEY");
        if let Err(e) = master_key_res {
            return Err(ErrorCode::SecretKeyNotSet(format!(
                "Secret key not found for azure blob client, {}",
                e.to_string()
            )));
        }

        let master_key = master_key_res.unwrap();
        let http_client: Arc<Box<dyn HttpClient>> = Arc::new(Box::new(reqwest::Client::new()));
        let client = StorageAccountClient::new_access_key(http_client, account, &master_key);

        Ok(Self {
            client: client.as_storage_client(),
            container: container.into(),
        })
    }

    pub fn with_credentials(
        account: impl Into<String>,
        container: impl Into<String>,
        master_key: impl Into<String>,
    ) -> Self {
        let http_client: Arc<Box<dyn HttpClient>> = Arc::new(Box::new(reqwest::Client::new()));
        let client = StorageAccountClient::new_access_key(http_client, account, master_key);

        Self {
            client: client.as_storage_client(),
            container: container.into(),
        }
    }

    async fn put_blob(&self, blob_name: &str, body: Vec<u8>) -> common_exception::Result<()> {
        let blob = self
            .client
            .as_container_client(&self.container)
            .as_blob_client(blob_name);

        let response_opt = blob.put_block_blob(body).execute().await;
        match response_opt {
            Err(e) => {
                return Err(ErrorCode::DALTransportError(format!(
                    "Failed on azure blob put operation, {}",
                    e.to_string()
                )))
            }
            Ok(_) => Ok(()),
        }
    }

    pub fn get_stream(&self, path: impl Into<String>) -> AzureBlobInputStream {
        let blob_client = self
            .client
            .clone()
            .as_container_client(&self.container)
            .as_blob_client(path);

        AzureBlobInputStream::create(blob_client)
    }
}

#[async_trait::async_trait]
impl DataAccessor for AzureBlobAccessor {
    fn get_input_stream(
        &self,
        path: &str,
        _stream_len: Option<u64>,
    ) -> common_exception::Result<InputStream> {
        let blob_client = self
            .client
            .clone()
            .as_container_client(&self.container)
            .as_blob_client(path);

        Ok(Box::new(AzureBlobInputStream::create(blob_client)))
    }

    /// Get blob as Bytes from Azure blob
    ///
    /// * `blob` - the blob name is corresponding to the BlobName in the example url 'https://myaccount.blob.core.windows.net/mycontainer/BlobName'
    async fn get(&self, blob: &str) -> common_exception::Result<Bytes> {
        let blob = self
            .client
            .as_container_client(&self.container)
            .as_blob_client(blob);

        let retrieved_blob_opt = blob.get().execute().await;
        match retrieved_blob_opt {
            Err(e) => {
                return Err(ErrorCode::DALTransportError(format!(
                    "Failed on azure blob get operation,, {}",
                    e.to_string()
                )));
            }
            Ok(blob_data) => Ok(blob_data.data),
        }
    }

    async fn put(&self, path: &str, content: Vec<u8>) -> common_exception::Result<()> {
        self.put_blob(path, content).await
    }

    async fn put_stream(
        &self,
        path: &str,
        input_stream: Box<
            dyn Stream<Item = std::result::Result<bytes::Bytes, std::io::Error>>
                + Send
                + Unpin
                + 'static,
        >,
        _stream_len: usize,
    ) -> common_exception::Result<()> {
        let mut data: Vec<u8> = vec![];
        let mut s = Box::pin(input_stream);
        while let Some(bytes_res) = s.next().await {
            match bytes_res {
                Err(e) => return Err(ErrorCode::DALTransportError(e.to_string())),
                Ok(bytes) => data.append(&mut bytes.to_vec()),
            }
        }
        self.put_blob(path, data).await
    }
}
