// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_exception::ErrorCode;
use common_exception::Result;
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
        log::error!("OnInit Error: {:?}", error);
        writer.error(ErrorKind::ER_UNKNOWN_ERROR, format!("{}", error).as_bytes())?;
        Ok(())
    }
}
