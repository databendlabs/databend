// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_exception::ErrorCode;
use common_exception::Result;
use std::sync::Arc;
use crate::sessions::ISession;

pub trait IMySQLEndpoint<Writer> {
    type Input;

    fn do_action(writer: Writer, session: Arc<Box<dyn ISession>>) -> Result<()>;

    fn ok(data: Self::Input, writer: Writer) -> Result<()>;

    fn err(error: &ErrorCode, writer: Writer) -> Result<()>;
}
