// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use async_trait::async_trait;
use common_datavalues::{DataField, DataSchema, DataType};
use common_planners::SettingPlan;

use crate::datastreams::{DataBlockStream, SendableDataBlockStream};
use crate::error::FuseQueryResult;
use crate::interpreters::IInterpreter;
use crate::sessions::FuseQueryContextRef;

pub struct SettingInterpreter {
    ctx: FuseQueryContextRef,
    set: SettingPlan,
}

impl SettingInterpreter {
    pub fn try_create(
        ctx: FuseQueryContextRef,
        set: SettingPlan,
    ) -> FuseQueryResult<Arc<dyn IInterpreter>> {
        Ok(Arc::new(SettingInterpreter { ctx, set }))
    }
}

#[async_trait]
impl IInterpreter for SettingInterpreter {
    fn name(&self) -> &str {
        "SettingInterpreter"
    }

    async fn execute(&self) -> FuseQueryResult<SendableDataBlockStream> {
        let plan = self.set.clone();
        for var in plan.vars {
            match var.variable.to_lowercase().as_str() {
                // To be compatible with some drivers
                // eg: usql and mycli
                "sql_mode" | "autocommit" => {}
                _ => {
                    self.ctx.update_settings(&var.variable, var.value)?;
                }
            }
        }

        let schema = Arc::new(DataSchema::new(vec![DataField::new(
            "set",
            DataType::Utf8,
            false,
        )]));
        Ok(Box::pin(DataBlockStream::create(schema, None, vec![])))
    }
}
