// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt::Write;
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::BlockEntry;
use databend_common_expression::DataBlock;
use databend_common_expression::Scalar;
use databend_common_expression::Value;
use databend_common_sql::plans::ShowCreateDatabasePlan;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub struct ShowCreateDatabaseInterpreter {
    ctx: Arc<QueryContext>,
    plan: ShowCreateDatabasePlan,
}

impl ShowCreateDatabaseInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: ShowCreateDatabasePlan) -> Result<Self> {
        Ok(ShowCreateDatabaseInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for ShowCreateDatabaseInterpreter {
    fn name(&self) -> &str {
        "ShowCreateDatabaseInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let tenant = self.ctx.get_tenant();
        let catalog = self.ctx.get_catalog(&self.plan.catalog).await?;
        let db = catalog.get_database(&tenant, &self.plan.database).await?;
        let name = db.name();
        let mut info = format!("CREATE DATABASE `{}`", name);
        if !db.engine().is_empty() {
            let engine = format!(" ENGINE={}", db.engine().to_uppercase());
            let engine_options = db
                .engine_options()
                .iter()
                .map(|(k, v)| format!("{}='{}'", k, v))
                .collect::<Vec<_>>()
                .join(", ");
            if !engine_options.is_empty() {
                write!(info, "{}({})", engine, engine_options)
                    .expect("write to string must succeed");
            } else {
                info.push_str(&engine);
            }
        }

        PipelineBuildResult::from_blocks(vec![DataBlock::new(
            vec![
                BlockEntry::new(
                    DataType::String,
                    Value::Scalar(Scalar::String(name.to_string())),
                ),
                BlockEntry::new(
                    DataType::String,
                    Value::Scalar(Scalar::String(info.clone())),
                ),
            ],
            1,
        )])
    }
}
