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

use std::{collections::BTreeMap, path::Path};

use reqwest::{Body, Client as HttpClient, StatusCode};
use tokio::io::AsyncRead;
use tokio::io::AsyncWriteExt;
use tokio_stream::StreamExt;
use tokio_util::io::ReaderStream;

use crate::error::{Error, Result};

pub type Reader = Box<dyn AsyncRead + Send + Sync + Unpin + 'static>;

#[derive(Debug, Clone)]
pub enum PresignMode {
    Auto,
    Detect,
    On,
    Off,
}

pub struct PresignedResponse {
    pub method: String,
    pub headers: BTreeMap<String, String>,
    pub url: String,
}

pub async fn presign_upload_to_stage(
    presigned: PresignedResponse,
    data: Reader,
    size: u64,
) -> Result<()> {
    let client = HttpClient::new();
    let mut builder = client.put(presigned.url);
    for (k, v) in presigned.headers {
        if k.to_lowercase() == "content-length" {
            continue;
        }
        builder = builder.header(k, v);
    }
    builder = builder.header("Content-Length", size.to_string());
    let stream = Body::wrap_stream(ReaderStream::new(data));
    let resp = builder.body(stream).send().await?;
    let status = resp.status();
    let body = resp.bytes().await?;
    match status {
        StatusCode::OK => Ok(()),
        _ => Err(Error::IO(format!(
            "Upload with presigned url failed: {}",
            String::from_utf8_lossy(&body)
        ))),
    }
}

pub async fn presign_download_from_stage(
    presigned: PresignedResponse,
    local_path: &Path,
) -> Result<u64> {
    if let Some(p) = local_path.parent() {
        tokio::fs::create_dir_all(p).await?;
    }
    let client = HttpClient::new();
    let mut builder = client.get(presigned.url);
    for (k, v) in presigned.headers {
        builder = builder.header(k, v);
    }

    let resp = builder.send().await?;
    let status = resp.status();
    match status {
        StatusCode::OK => {
            let mut file = tokio::fs::File::create(local_path).await?;
            let mut body = resp.bytes_stream();
            while let Some(chunk) = body.next().await {
                file.write_all(&chunk?).await?;
            }
            file.flush().await?;
            let metadata = file.metadata().await?;
            Ok(metadata.len())
        }
        _ => Err(Error::IO(format!(
            "Download with presigned url failed: {}",
            status
        ))),
    }
}
