// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_datavalues::DataSchema;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataValue;
use common_exception::Result;
use common_planners::Expression;
use common_planners::ReadDataSourcePlan;
use common_planners::ScanPlan;

use crate::datasources::IDataSource;
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
        let datasource = crate::datasources::DataSource::try_create()?;
        let table = datasource.get_table(self.db, self.table)?;
        table.schema()
    }

    pub fn number_read_source_plan_for_test(&self, numbers: i64) -> Result<ReadDataSourcePlan> {
        let datasource = crate::datasources::DataSource::try_create()?;
        let table = datasource.get_table(self.db, self.table)?;
        table.read_plan(
            self.ctx.clone(),
            &ScanPlan {
                schema_name: self.db.to_string(),
                table_schema: Arc::new(DataSchema::empty()),
                table_args: Some(Expression::Literal(DataValue::Int64(Some(numbers)))),
                projection: None,
                projected_schema: Arc::new(DataSchema::empty()),
                filters: vec![],
                limit: None,
            },
            self.ctx.get_max_threads()? as usize,
        )
    }

    pub fn number_source_transform_for_test(&self, numbers: i64) -> Result<SourceTransform> {
        let plan = self.number_read_source_plan_for_test(numbers)?;
        self.ctx.try_set_partitions(plan.partitions)?;
        SourceTransform::try_create(self.ctx.clone(), self.db, self.table, false)
    }
}
