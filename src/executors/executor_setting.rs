// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use async_trait::async_trait;
use log::debug;
use std::sync::Arc;

use crate::contexts::FuseQueryContextRef;
use crate::datastreams::{DataBlockStream, SendableDataBlockStream};
use crate::datavalues::{DataField, DataSchema, DataType};
use crate::error::{FuseQueryError, FuseQueryResult};
use crate::executors::IExecutor;
use crate::planners::SettingPlan;

pub struct SettingExecutor {
    ctx: FuseQueryContextRef,
    set: SettingPlan,
}

impl SettingExecutor {
    pub fn try_create(
        ctx: FuseQueryContextRef,
        set: SettingPlan,
    ) -> FuseQueryResult<Arc<dyn IExecutor>> {
        Ok(Arc::new(SettingExecutor { ctx, set }))
    }
}

#[async_trait]
impl IExecutor for SettingExecutor {
    fn name(&self) -> &str {
        "SetVariableExecutor"
    }

    async fn execute(&self) -> FuseQueryResult<SendableDataBlockStream> {
        let plan = self.set.clone();
        match plan.variable.to_lowercase().as_str() {
            "max_block_size" => {
                let value = plan.value.parse::<u64>()?;
                self.ctx.set_max_block_size(value)?;
            }
            "max_threads" => {
                let value = plan.value.parse::<u64>()?;
                self.ctx.set_max_threads(value)?;
            }
            _ => {
                return Err(FuseQueryError::Internal(format!(
                    "Unknown variable: {:?}",
                    plan.variable
                )))
            }
        }
        debug!("Set variable executor: {:?}", self.ctx);

        let schema = Arc::new(DataSchema::new(vec![DataField::new(
            "set",
            DataType::Utf8,
            false,
        )]));
        Ok(Box::pin(DataBlockStream::create(schema, None, vec![])))
    }
}
