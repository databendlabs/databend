// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

// use anyhow::Result;
use common_exception::Result;
use common_streams::SendableDataBlockStream;

#[async_trait::async_trait]
pub trait IInterpreter: Sync + Send {
    fn name(&self) -> &str;
    async fn execute(&self) -> Result<SendableDataBlockStream>;
}

pub type InterpreterPtr = std::sync::Arc<dyn IInterpreter>;
