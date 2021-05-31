// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_datavalues::DataSchemaRef;
use common_datavalues::DataSchemaRefExt;
use common_exception::Result;
use common_streams::SendableDataBlockStream;

#[async_trait::async_trait]
pub trait IInterpreter: Sync + Send {
    fn name(&self) -> &str;
    fn schema(&self) -> DataSchemaRef {
        DataSchemaRefExt::create(vec![])
    }
    async fn execute(&self) -> Result<SendableDataBlockStream>;
}

pub type InterpreterPtr = std::sync::Arc<dyn IInterpreter>;
