// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use async_trait::async_trait;

use crate::datastreams::SendableDataBlockStream;
use crate::error::FuseQueryResult;

#[async_trait]
pub trait IInterpreter: Sync + Send {
    fn name(&self) -> &str;
    async fn execute(&self) -> FuseQueryResult<SendableDataBlockStream>;
}
