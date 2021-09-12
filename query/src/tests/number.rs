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

use std::sync::Arc;

use common_datavalues::DataSchema;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataValue;
use common_exception::Result;
use common_planners::Expression;
use common_planners::Extras;
use common_planners::ReadDataSourcePlan;
use common_planners::ScanPlan;

use crate::catalogs::Catalog;
use crate::pipelines::transforms::SourceTransform;
use crate::sessions::DatafuseQueryContextRef;
use crate::tests::try_create_catalog;

pub struct NumberTestData {
    ctx: DatafuseQueryContextRef,
    db: &'static str,
    table: &'static str,
}

impl NumberTestData {
    pub fn create(ctx: DatafuseQueryContextRef) -> Self {
        NumberTestData {
            ctx,
            db: "system",
            table: "numbers_mt",
        }
    }

    pub fn number_schema_for_test(&self) -> Result<DataSchemaRef> {
        let catalog = try_create_catalog()?;
        catalog.get_table(self.db, self.table)?.raw().schema()
    }

    pub fn number_read_source_plan_for_test(&self, numbers: i64) -> Result<ReadDataSourcePlan> {
        let catalog = try_create_catalog()?;
        let table_meta = catalog.get_table(self.db, self.table)?;
        let table = table_meta.raw();
        table.read_plan(
            self.ctx.clone(),
            &ScanPlan {
                schema_name: self.db.to_string(),
                table_id: table_meta.meta_id(),
                table_version: table_meta.meta_ver(),
                table_schema: Arc::new(DataSchema::empty()),
                table_args: Some(Expression::create_literal(DataValue::Int64(Some(numbers)))),
                projected_schema: Arc::new(DataSchema::empty()),
                push_downs: Extras::default(),
            },
            self.ctx.get_settings().get_max_threads()? as usize,
        )
    }

    pub fn number_source_transform_for_test(&self, numbers: i64) -> Result<SourceTransform> {
        let source_plan = self.number_read_source_plan_for_test(numbers)?;
        self.ctx.try_set_partitions(source_plan.parts.clone())?;
        SourceTransform::try_create(self.ctx.clone(), source_plan)
    }
}
