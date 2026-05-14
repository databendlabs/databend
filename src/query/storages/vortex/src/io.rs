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

//! IO bridge: opendal::Operator → VortexReadAt
//!
//! We implement VortexReadAt directly on top of opendal::Operator, bypassing
//! the object_store version conflict (workspace uses 0.12, vortex-io needs 0.13).

use std::sync::Arc;

use futures::FutureExt;
use futures::future::BoxFuture;
use opendal::Operator;
use vortex_array::buffer::BufferHandle;
use vortex_buffer::Alignment;
use vortex_buffer::ByteBuffer;
use vortex_error::VortexResult;

use vortex_io::VortexReadAt;

/// Wraps an opendal Operator as a VortexReadAt source.
#[derive(Clone)]
pub struct OpendalVortexReader {
    operator: Operator,
    path: Arc<String>,
}

impl OpendalVortexReader {
    pub fn new(operator: Operator, path: impl Into<String>) -> Self {
        Self {
            operator,
            path: Arc::new(path.into()),
        }
    }
}

impl VortexReadAt for OpendalVortexReader {
    fn read_at(
        &self,
        offset: u64,
        length: usize,
        _alignment: Alignment,
    ) -> BoxFuture<'static, VortexResult<BufferHandle>> {
        let operator = self.operator.clone();
        let path = self.path.clone();

        async move {
            let end = offset + length as u64;
            let buf = operator
                .read_with(&*path)
                .range(offset..end)
                .await
                .map_err(|e| vortex_error::vortex_err!("opendal read_at error: {e}"))?;

            let byte_buf = ByteBuffer::from(buf.to_bytes());
            Ok(BufferHandle::new_host(byte_buf))
        }
        .boxed()
    }

    fn size(&self) -> BoxFuture<'static, VortexResult<u64>> {
        let operator = self.operator.clone();
        let path = self.path.clone();

        async move {
            let meta = operator
                .stat(&*path)
                .await
                .map_err(|e| vortex_error::vortex_err!("opendal stat error: {e}"))?;
            Ok(meta.content_length())
        }
        .boxed()
    }

    fn concurrency(&self) -> usize {
        64
    }
}
