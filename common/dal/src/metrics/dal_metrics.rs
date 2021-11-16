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

use common_metrics::label_counter_with_val;
use common_metrics::TenantLabel;
use futures::Stream;

use crate::metrics::stream_metrics::InputStreamWithMetric;
use crate::metrics::METRIC_DAL_WRITE_BYTES;
use crate::AsyncSeekableReader;
use crate::DataAccessor;
use crate::InputStream;

pub struct DalWithMetric {
    tenant_label: TenantLabel,
    inner: Arc<dyn DataAccessor>,
}

impl DalWithMetric {
    pub fn new(tenant_label: TenantLabel, inner: Arc<dyn DataAccessor>) -> Self {
        Self {
            tenant_label,
            inner,
        }
    }
}

#[async_trait::async_trait]
impl DataAccessor for DalWithMetric {
    fn get_input_stream(
        &self,
        path: &str,
        stream_len: Option<u64>,
    ) -> common_exception::Result<InputStream> {
        self.inner
            .get_input_stream(path, stream_len)
            .map(|input_stream| {
                let r = InputStreamWithMetric::new(self.tenant_label.clone(), input_stream);
                Box::new(r) as Box<dyn AsyncSeekableReader + Unpin + Send>
            })
    }

    async fn put(&self, path: &str, content: Vec<u8>) -> common_exception::Result<()> {
        let len = content.len();
        self.inner.put(path, content).await.map(|_| {
            label_counter_with_val(
                METRIC_DAL_WRITE_BYTES,
                len as u64,
                self.tenant_label.tenant_id.as_str(),
                self.tenant_label.cluster_id.as_str(),
            )
        })
    }

    async fn put_stream(
        &self,
        path: &str,
        input_stream: Box<
            dyn Stream<Item = Result<bytes::Bytes, std::io::Error>> + Send + Unpin + 'static,
        >,
        stream_len: usize,
    ) -> common_exception::Result<()> {
        self.inner
            .put_stream(path, input_stream, stream_len)
            .await
            .map(|_| {
                label_counter_with_val(
                    METRIC_DAL_WRITE_BYTES,
                    stream_len as u64,
                    self.tenant_label.tenant_id.as_str(),
                    self.tenant_label.cluster_id.as_str(),
                )
            })
    }
}
