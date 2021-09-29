// Copyright 2020 Datafuse Labs.
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

use common_datavalues::DataSchemaRef;
use common_datavalues::DataValue;
use common_exception::Result;
use common_planners::Expression;
use common_planners::ReadDataSourcePlan;

use crate::catalogs::Catalog;
use crate::pipelines::transforms::SourceTransform;
use crate::sessions::DatabendQueryContextRef;
use crate::tests::try_create_catalog;

pub struct NumberTestData {
    ctx: DatabendQueryContextRef,
    table: &'static str,
}

impl NumberTestData {
    pub fn create(ctx: DatabendQueryContextRef) -> Self {
        NumberTestData {
            ctx,
            table: "numbers_mt",
        }
    }

    pub fn number_schema_for_test(&self) -> Result<DataSchemaRef> {
        let catalog = try_create_catalog()?;
        let tbl_arg = Some(vec![Expression::create_literal(DataValue::Int64(Some(1)))]);
        catalog
            .get_table_function(self.table, tbl_arg)?
            .raw()
            .schema()
    }

    pub fn number_read_source_plan_for_test(&self, numbers: i64) -> Result<ReadDataSourcePlan> {
        let catalog = try_create_catalog()?;
        let tbl_arg = Some(vec![Expression::create_literal(DataValue::Int64(Some(
            numbers,
        )))]);
        let table_meta = catalog.get_table_function(self.table, tbl_arg)?;
        let table = table_meta.raw();
        table.read_plan(
            self.ctx.clone(),
            None,
            Some(self.ctx.get_settings().get_max_threads()? as usize),
        )
    }

    pub fn number_source_transform_for_test(&self, numbers: i64) -> Result<SourceTransform> {
        let source_plan = self.number_read_source_plan_for_test(numbers)?;
        self.ctx.try_set_partitions(source_plan.parts.clone())?;
        SourceTransform::try_create(self.ctx.clone(), source_plan)
    }
}
