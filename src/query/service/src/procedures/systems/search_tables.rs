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

use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::DataBlock;
use common_expression::DataSchema;
use common_procedures::ProcedureFeatures;
use common_procedures::ProcedureSignature;
use futures::TryStreamExt;

use crate::interpreters::Interpreter;
use crate::interpreters::SelectInterpreter;
use crate::procedures::OneBlockProcedure;
use crate::procedures::Procedure;
use crate::sessions::QueryContext;
use crate::sql::plans::Plan;
use crate::sql::Planner;

pub struct SearchTablesProcedure {
    sig: Box<dyn ProcedureSignature>,
}

impl SearchTablesProcedure {
    pub fn try_create(sig: Box<dyn ProcedureSignature>) -> Result<Box<dyn Procedure>> {
        Ok(SearchTablesProcedure { sig }.into_procedure())
    }
}

impl ProcedureSignature for SearchTablesProcedure {
    fn name(&self) -> &str {
        self.sig.name()
    }

    fn features(&self) -> ProcedureFeatures {
        self.sig.features()
    }

    fn schema(&self) -> Arc<DataSchema> {
        self.sig.schema()
    }
}

#[async_trait::async_trait]
impl OneBlockProcedure for SearchTablesProcedure {
    #[async_backtrace::framed]
    async fn all_data(&self, ctx: Arc<QueryContext>, args: Vec<String>) -> Result<DataBlock> {
        assert_eq!(args.len(), 1);
        let query = format!(
            "SELECT * FROM system.tables WHERE name like '%{}%' ORDER BY database, name",
            args[0]
        );
        let mut planner = Planner::new(ctx.clone());
        let (plan, _) = planner.plan_sql(&query).await?;

        let stream = if let Plan::Query {
            s_expr,
            metadata,
            bind_context,
            ..
        } = plan
        {
            let interpreter = SelectInterpreter::try_create(
                ctx.clone(),
                *bind_context,
                *s_expr,
                metadata,
                None,
                false,
            )?;
            interpreter.execute(ctx.clone()).await
        } else {
            return Err(ErrorCode::Internal("search tables build query error"));
        }?;
        let result = stream.try_collect::<Vec<_>>().await?;
        if !result.is_empty() {
            Ok(result[0].clone())
        } else {
            Ok(DataBlock::empty())
        }
    }
}
