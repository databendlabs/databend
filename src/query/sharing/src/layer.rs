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

use std::fmt::Debug;
use std::io::Error;
use std::io::ErrorKind;
use std::io::Result;
use std::sync::Arc;

use async_trait::async_trait;
use http::Request;
use opendal::http_util::AsyncBody;
use opendal::http_util::HttpClient;
use opendal::ops::BytesRange;
use opendal::ops::OpRead;
use opendal::ops::Operation;
use opendal::Accessor;
use opendal::AccessorCapability;
use opendal::AccessorMetadata;
use opendal::BytesReader;
use opendal::Layer;
use opendal::Scheme;
use reqwest::header::RANGE;

use crate::SharedSinger;

/// SharedLayer is used to handle databend cloud's sharing logic.
///
/// We will inject all read request to:
///
/// - Get presgined url from sharing endpoint.
/// - Read data from the url instead.
///
/// # Example:
///
/// ```no_build
/// use anyhow::Result;
/// use common_sharing::SharedLayer;
/// use opendal::Operator;
/// use opendal::Scheme;
///
/// let _ = Operator::from_env(Scheme::Memory)
///     .expect("must init")
///     .layer(SharedLayer::new(signer));
/// ```
#[derive(Debug, Clone)]
pub struct SharedLayer {
    signer: SharedSinger,
}

impl SharedLayer {
    /// Create a new SharedLayer.
    pub fn new(signer: SharedSinger) -> Self {
        Self { signer }
    }
}

impl Layer for SharedLayer {
    fn layer(&self, _: Arc<dyn Accessor>) -> Arc<dyn Accessor> {
        Arc::new(SharedAccessor {
            signer: self.signer.clone(),
            client: HttpClient::new(),
        })
    }
}

#[derive(Debug)]
struct SharedAccessor {
    signer: SharedSinger,
    client: HttpClient,
}

#[async_trait]
impl Accessor for SharedAccessor {
    fn metadata(&self) -> AccessorMetadata {
        let mut meta = AccessorMetadata::default();
        meta.set_scheme(Scheme::Custom("shared"))
            .set_capabilities(AccessorCapability::Read);
        meta
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<BytesReader> {
        let mut req: Request<AsyncBody> = self
            .signer
            .fetch(path, Operation::Read)
            .await
            .map_err(|err| Error::new(ErrorKind::Other, err))?
            .into();

        req.headers_mut().insert(
            RANGE,
            BytesRange::new(args.offset(), args.size())
                .to_string()
                .parse()
                .map_err(|err| Error::new(ErrorKind::Other, err))?,
        );

        let resp = self.client.send_async(req).await?;
        Ok(resp.into_body().reader())
    }
}
