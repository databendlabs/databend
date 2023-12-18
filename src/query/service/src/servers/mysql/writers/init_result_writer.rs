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

use databend_common_base::base::tokio::io::AsyncWrite;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use log::error;
use opensrv_mysql::*;

pub struct DFInitResultWriter<'a, W: AsyncWrite + Send + Unpin> {
    inner: Option<InitWriter<'a, W>>,
}

impl<'a, W: AsyncWrite + Send + Unpin> DFInitResultWriter<'a, W> {
    pub fn create(inner: InitWriter<'a, W>) -> DFInitResultWriter<'a, W> {
        DFInitResultWriter::<'a, W> { inner: Some(inner) }
    }

    #[async_backtrace::framed]
    pub async fn write(&mut self, query_result: Result<()>) -> Result<()> {
        if let Some(writer) = self.inner.take() {
            match query_result {
                Ok(_) => Self::ok(writer).await?,
                Err(error) => Self::err(&error, writer).await?,
            }
        }

        Ok(())
    }

    #[async_backtrace::framed]
    async fn ok(writer: InitWriter<'a, W>) -> Result<()> {
        writer.ok().await?;
        Ok(())
    }

    #[async_backtrace::framed]
    async fn err(error: &ErrorCode, writer: InitWriter<'a, W>) -> Result<()> {
        error!("OnInit Error: {:?}", error);
        writer
            .error(ErrorKind::ER_UNKNOWN_ERROR, error.to_string().as_bytes())
            .await?;
        Ok(())
    }
}
