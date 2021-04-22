// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use anyhow::Result;
use common_datavalues::DataField;
use common_datavalues::DataSchema;
use common_datavalues::DataType;
use common_planners::SettingPlan;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;

use crate::interpreters::IInterpreter;
use crate::sessions::FuseQueryContextRef;

pub struct SettingInterpreter {
    ctx: FuseQueryContextRef,
    set: SettingPlan
}

impl SettingInterpreter {
    pub fn try_create(ctx: FuseQueryContextRef, set: SettingPlan) -> Result<Arc<dyn IInterpreter>> {
        Ok(Arc::new(SettingInterpreter { ctx, set }))
    }
}

#[async_trait::async_trait]
impl IInterpreter for SettingInterpreter {
    fn name(&self) -> &str {
        "SettingInterpreter"
    }

    async fn execute(&self) -> Result<SendableDataBlockStream> {
        let plan = self.set.clone();
        for var in plan.vars {
            match var.variable.to_lowercase().as_str() {
                // To be compatible with some drivers
                "sql_mode" | "autocommit" => {}
                _ => {
                    self.ctx.update_settings(&var.variable, var.value)?;
                }
            }
        }

        let schema = Arc::new(DataSchema::new(vec![DataField::new(
            "set",
            DataType::Utf8,
            false
        )]));
        Ok(Box::pin(DataBlockStream::create(schema, None, vec![])))
    }
}
