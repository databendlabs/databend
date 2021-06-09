// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_exception::ErrorCode;
use common_exception::Result;

pub trait IMySQLEndpoint<Writer> {
    type Input;

    fn ok(data: Self::Input, writer: Writer) -> Result<()>;

    fn err(error: ErrorCode, writer: Writer) -> Result<()>;
}
