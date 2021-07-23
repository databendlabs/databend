// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_datavalues::DataSchema;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataValue;
use common_exception::Result;
use common_planners::Expression;
use common_planners::Extras;
use common_planners::ReadDataSourcePlan;
use common_planners::ScanPlan;

use crate::pipelines::transforms::SourceTransform;
use crate::sessions::FuseQueryContextRef;

pub struct NumberTestData {
    ctx: FuseQueryContextRef,
    db: &'static str,
    table: &'static str,
}

impl NumberTestData {
    pub fn create(ctx: FuseQueryContextRef) -> Self {
        NumberTestData {
            ctx,
            db: "system",
            table: "numbers_mt",
        }
    }

    pub fn number_schema_for_test(&self) -> Result<DataSchemaRef> {
        let datasource = crate::datasources::DatabaseCatalog::try_create()?;
        datasource
            .get_table(self.db, self.table)?
            .datasource()
            .schema()
    }

    pub fn number_read_source_plan_for_test(&self, numbers: i64) -> Result<ReadDataSourcePlan> {
        let datasource = crate::datasources::DatabaseCatalog::try_create()?;
        let table_meta = datasource.get_table(self.db, self.table)?;
        let table = table_meta.datasource();
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
        SourceTransform::try_create(self.ctx.clone(), source_plan.clone())
    }
}
