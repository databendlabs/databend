// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_exception::ErrorCodes;

pub trait IMySQLEndpoint<Writer> {
    type Input;

    fn ok(data: Self::Input, writer: Writer) -> std::io::Result<()>;

    fn err(error: ErrorCodes, writer: Writer) -> std::io::Result<()>;
}
