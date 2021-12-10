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

use common_exception::ErrorCode;
use common_exception::Result;
use common_tracing::tracing;
use msql_srv::*;

pub struct DFInitResultWriter<'a, W: std::io::Write> {
    inner: Option<InitWriter<'a, W>>,
}

impl<'a, W: std::io::Write> DFInitResultWriter<'a, W> {
    pub fn create(inner: InitWriter<'a, W>) -> DFInitResultWriter<'a, W> {
        DFInitResultWriter::<'a, W> { inner: Some(inner) }
    }

    pub fn write(&mut self, query_result: Result<()>) -> Result<()> {
        if let Some(writer) = self.inner.take() {
            match query_result {
                Ok(_) => Self::ok(writer)?,
                Err(error) => Self::err(&error, writer)?,
            }
        }

        Ok(())
    }

    fn ok(writer: InitWriter<'a, W>) -> Result<()> {
        writer.ok()?;
        Ok(())
    }

    fn err(error: &ErrorCode, writer: InitWriter<'a, W>) -> Result<()> {
        tracing::error!("OnInit Error: {:?}", error);
        writer.error(ErrorKind::ER_UNKNOWN_ERROR, format!("{}", error).as_bytes())?;
        Ok(())
    }
}
