// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use msql_srv::*;

use crate::servers::mysql::endpoints::IMySQLEndpoint;
use common_exception::Result;
use common_exception::ErrorCode;

pub struct MySQLOnInitEndpoint;

impl<'a, T: std::io::Write> IMySQLEndpoint<InitWriter<'a, T>> for MySQLOnInitEndpoint {
    type Input = ();

    fn ok(data: Self::Input, writer: InitWriter<'a, T>) -> Result<()> {
        writer.ok()?;
        Ok(())
    }

    fn err(error: &ErrorCode, writer: InitWriter<'a, T>) -> Result<()> {
        log::error!("OnInit Error: {:?}", error);
        writer.error(ErrorKind::ER_UNKNOWN_ERROR, format!("{}", error).as_bytes())?;
        Ok(())
    }
}
