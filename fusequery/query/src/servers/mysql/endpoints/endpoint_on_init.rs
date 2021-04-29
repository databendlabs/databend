// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_exception::ErrorCodes;
use msql_srv::ErrorKind;
use msql_srv::InitWriter;

use crate::servers::mysql::endpoints::IMySQLEndpoint;

struct MySQLOnInitEndpoint;

impl<'a, T: std::io::Write> IMySQLEndpoint<InitWriter<'a, T>> for MySQLOnInitEndpoint {
    type Input = ();

    fn ok(_data: Self::Input, writer: InitWriter<'a, T>) -> std::io::Result<()> {
        writer.ok()
    }

    fn err(error: ErrorCodes, writer: InitWriter<'a, T>) -> std::io::Result<()> {
        writer.error(ErrorKind::ER_UNKNOWN_ERROR, format!("{}", error).as_bytes())
    }
}

type Input = anyhow::Result<(), ErrorCodes>;
type Output = std::io::Result<()>;

// TODO: Maybe can use generic to abstract all MySQLEndpoints done function
pub fn done<W: std::io::Write>(writer: InitWriter<'_, W>) -> impl FnOnce(Input) -> Output + '_ {
    move |res: Input| -> Output {
        match res {
            Err(error) => MySQLOnInitEndpoint::err(error, writer),
            Ok(value) => MySQLOnInitEndpoint::ok(value, writer)
        }
    }
}
