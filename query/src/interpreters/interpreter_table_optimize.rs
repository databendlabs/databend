// Copyright 2021 Datafuse Labs.
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

use common_exception::Result;
use common_planners::Optimization;
use common_planners::OptimizeTablePlan;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;
use futures::StreamExt;

use crate::catalogs::Catalog;
use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterFactory;
use crate::interpreters::InterpreterPtr;
use crate::sessions::QueryContext;
use crate::sql::PlanParser;

pub struct OptimizeTableInterpreter {
    ctx: Arc<QueryContext>,
    plan: OptimizeTablePlan,
}

impl OptimizeTableInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: OptimizeTablePlan) -> Result<InterpreterPtr> {
        Ok(Arc::new(OptimizeTableInterpreter { ctx, plan }))
    }
}

#[async_trait::async_trait]
impl Interpreter for OptimizeTableInterpreter {
    fn name(&self) -> &str {
        "OptimizeTableInterpreter"
    }

    async fn execute(
        &self,
        _input_stream: Option<SendableDataBlockStream>,
    ) -> Result<SendableDataBlockStream> {
        let plan = &self.plan;
        let mut table = self.ctx.get_table(&plan.database, &plan.table).await?;
        let operation = &plan.operation;

        let do_purge = operation.contains(Optimization::PURGE);
        let do_compact = operation.contains(Optimization::COMPACT);

        if do_compact {
            // it is a "simple and violent" strategy, to be optimized later
            let obj_name = format!("{}.{}", &plan.database, &plan.table);
            let rewritten_query =
                format!("INSERT OVERWRITE {} SELECT * FROM {}", obj_name, obj_name);
            let rewritten_plan =
                PlanParser::parse(self.ctx.clone(), rewritten_query.as_str()).await?;
            let interpreter = InterpreterFactory::get(self.ctx.clone(), rewritten_plan)?;
            let mut stream = interpreter.execute(None).await?;
            while let Some(Ok(_)) = stream.next().await {}
            if do_purge {
                // currently, context caches the table, we have to "refresh"
                // the table by using the catalog API directly
                let tenant = self.ctx.get_tenant();
                table = self
                    .ctx
                    .get_catalog()
                    .get_table(tenant.as_str(), &plan.database, &plan.table)
                    .await?;
            }
        }

        if do_purge {
            table.optimize(self.ctx.clone(), true).await?;
        }

        Ok(Box::pin(DataBlockStream::create(
            self.plan.schema(),
            None,
            vec![],
        )))
    }
}
