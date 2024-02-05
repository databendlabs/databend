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

use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::types::number::NumberScalar;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::BlockEntry;
use databend_common_expression::DataBlock;
use databend_common_expression::Scalar;
use databend_common_expression::Value;
use databend_common_sql::plans::ExistsTablePlan;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub struct ExistsTableInterpreter {
    ctx: Arc<QueryContext>,
    plan: ExistsTablePlan,
}

impl ExistsTableInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: ExistsTablePlan) -> Result<Self> {
        Ok(ExistsTableInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for ExistsTableInterpreter {
    fn name(&self) -> &str {
        "ExistsTableInterpreter"
    }

    fn is_ddl(&self) -> bool {
        false
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let catalog = self.plan.catalog.as_str();
        let database = self.plan.database.as_str();
        let table = self.plan.table.as_str();
        let exists = self.ctx.get_table(catalog, database, table).await.is_ok();
        let result = match exists {
            true => 1u8,
            false => 0u8,
        };

        PipelineBuildResult::from_blocks(vec![DataBlock::new(
            vec![BlockEntry::new(
                DataType::Number(NumberDataType::UInt8),
                Value::Scalar(Scalar::Number(NumberScalar::UInt8(result))),
            )],
            1,
        )])
    }
}
