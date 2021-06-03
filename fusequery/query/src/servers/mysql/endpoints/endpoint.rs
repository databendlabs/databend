// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_exception::ErrorCodes;
use common_exception::Result;

pub trait IMySQLEndpoint<Writer> {
    type Input;

    fn on_query<F: Fn() -> Result<Self::Input>>(writer: Writer, fun: F) -> Result<()> {
        match fun() {
            Ok(data) => Self::on_query_ok(data, writer),
            Err(error) => Self::on_query_err(error, writer)
        }
    }

    fn on_query_ok(data: Self::Input, writer: Writer) -> Result<()>;

    fn on_query_err(error: ErrorCodes, writer: Writer) -> Result<()>;
}
