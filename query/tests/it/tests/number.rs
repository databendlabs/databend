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

use common_datavalues::DataSchemaRef;
use common_datavalues::DataValue;
use common_exception::Result;
use common_planners::Expression;
use common_planners::ReadDataSourcePlan;
use databend_query::catalogs::Catalog;
use databend_query::pipelines::transforms::SourceTransform;
use databend_query::sessions::QueryContext;
use databend_query::storages::ToReadDataSourcePlan;

use crate::tests::create_catalog;

pub struct NumberTestData {
    ctx: Arc<QueryContext>,
    table: &'static str,
}

impl NumberTestData {
    pub fn create(ctx: Arc<QueryContext>) -> Self {
        NumberTestData {
            ctx,
            table: "numbers_mt",
        }
    }

    pub fn number_schema_for_test(&self) -> Result<DataSchemaRef> {
        let catalog = create_catalog()?;
        let tbl_arg = Some(vec![Expression::create_literal(DataValue::Int64(1))]);
        Ok(catalog.get_table_function(self.table, tbl_arg)?.schema())
    }

    pub fn number_read_source_plan_for_test(&self, numbers: i64) -> Result<ReadDataSourcePlan> {
        let catalog = create_catalog()?;
        futures::executor::block_on(async move {
            let tbl_arg = Some(vec![Expression::create_literal(DataValue::Int64(numbers))]);
            let table = catalog.get_table_function(self.table, tbl_arg)?;
            table
                .clone()
                .as_table()
                .read_plan(self.ctx.clone(), None)
                .await
        })
    }

    pub fn number_source_transform_for_test(&self, numbers: i64) -> Result<SourceTransform> {
        let source_plan = self.number_read_source_plan_for_test(numbers)?;
        self.ctx.try_set_partitions(source_plan.parts.clone())?;
        SourceTransform::try_create(self.ctx.clone(), source_plan)
    }
}
