// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use async_trait::async_trait;

use crate::datastreams::SendableDataBlockStream;
use crate::error::FuseQueryResult;

#[async_trait]
pub trait IExecutor: Sync + Send {
    fn name(&self) -> &str;
    async fn execute(&self) -> FuseQueryResult<SendableDataBlockStream>;
}
