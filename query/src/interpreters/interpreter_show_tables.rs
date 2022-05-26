// Copyright 2022 Datafuse Labs.
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
use common_planners::PlanNode;
use common_planners::PlanShowKind;
use common_planners::ShowTablesPlan;
use common_streams::SendableDataBlockStream;

use crate::catalogs::DatabaseCatalog;
use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::interpreters::SelectInterpreter;
use crate::optimizers::Optimizers;
use crate::sessions::QueryContext;
use crate::sql::PlanParser;

pub struct ShowTablesInterpreter {
    ctx: Arc<QueryContext>,
    plan: ShowTablesPlan,
}

impl ShowTablesInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: ShowTablesPlan) -> Result<InterpreterPtr> {
        Ok(Arc::new(ShowTablesInterpreter { ctx, plan }))
    }

    fn build_query(&self) -> Result<String> {
        let mut database = self.ctx.get_current_database();
        if let Some(v) = &self.plan.fromdb {
            database = v.to_string();
        }

        if DatabaseCatalog::is_case_insensitive_db(&database) {
            database = database.to_uppercase()
        }

        let showfull = self.plan.showfull;

        let mut select_builder = SimpleSelectBuilder::from("information_schema.tables");

        if showfull {
            select_builder
                .with_column(format!("table_name as Tables_in_{database}"))
                .with_column("table_type as Table_type")
                .with_column("table_catalog")
                .with_column("engine")
                .with_column("create_time");
            if self.plan.with_history {
                select_builder.with_column("drop_time");
            } else {
                select_builder
                    .with_column("num_rows")
                    .with_column("data_size")
                    .with_column("data_compressed_size")
                    .with_column("index_size");
            };
        } else {
            select_builder.with_column(format!("table_name as Tables_in_{database}"));
            if self.plan.with_history {
                select_builder.with_column("drop_time");
            };
        }

        select_builder
            .with_order_by("table_schema")
            .with_order_by("table_name");

        // filter out dropped tables if not showing history
        if !self.plan.with_history {
            select_builder.with_filter("drop_time = 'NULL'");
        };

        match &self.plan.kind {
            PlanShowKind::All => {
                select_builder.with_filter(format!("table_schema = '{database}'"));
                Ok(select_builder.build())
            }
            PlanShowKind::Like(v) => {
                select_builder
                    .with_filter(format!("table_schema = '{database}'"))
                    .with_filter(format!("table_name LIKE {v}"));
                Ok(select_builder.build())
            }
            PlanShowKind::Where(v) => {
                select_builder
                    .with_filter(format!("table_schema = '{database}'"))
                    .with_filter(format!("({v})"));
                Ok(select_builder.build())
            }
        }
    }
}

#[async_trait::async_trait]
impl Interpreter for ShowTablesInterpreter {
    fn name(&self) -> &str {
        "ShowTablesInterpreter"
    }

    async fn execute(
        &self,
        input_stream: Option<SendableDataBlockStream>,
    ) -> Result<SendableDataBlockStream> {
        let query = self.build_query()?;
        let plan = PlanParser::parse(self.ctx.clone(), &query).await?;
        let optimized = Optimizers::create(self.ctx.clone()).optimize(&plan)?;

        if let PlanNode::Select(plan) = optimized {
            let interpreter = SelectInterpreter::try_create(self.ctx.clone(), plan)?;
            interpreter.execute(input_stream).await
        } else {
            return Err(ErrorCode::LogicalError("Show tables build query error"));
        }
    }
}

struct SimpleSelectBuilder {
    from: String,
    columns: Vec<String>,
    filters: Vec<String>,
    order_bys: Vec<String>,
}

impl SimpleSelectBuilder {
    fn from(table_name: &str) -> SimpleSelectBuilder {
        SimpleSelectBuilder {
            from: table_name.to_owned(),
            columns: vec![],
            filters: vec![],
            order_bys: vec![],
        }
    }
    fn with_column(&mut self, col_name: impl Into<String>) -> &mut Self {
        self.columns.push(col_name.into());
        self
    }

    fn with_filter(&mut self, col_name: impl Into<String>) -> &mut Self {
        self.filters.push(col_name.into());
        self
    }

    fn with_order_by(&mut self, order_by: &str) -> &mut Self {
        self.order_bys.push(order_by.to_owned());
        self
    }

    fn build(self) -> String {
        let columns = {
            let s = self.columns.join(",");
            if !s.is_empty() {
                s
            } else {
                "*".to_owned()
            }
        };

        let order_bys = {
            let s = self.order_bys.join(",");
            if !s.is_empty() {
                format!("ORDER BY {s}")
            } else {
                s
            }
        };

        let filters = {
            let s = self.filters.join(" and ");
            if !s.is_empty() {
                format!("where {s}")
            } else {
                "".to_owned()
            }
        };

        let from = self.from;
        format!("SELECT {columns} FROM {from} {filters} {order_bys} ")
    }
}
