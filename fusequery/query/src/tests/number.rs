// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use anyhow::Result;
use common_datavalues::{DataSchema, DataSchemaRef, DataValue};
use common_planners::{ExpressionPlan, PlanNode, ReadDataSourcePlan, ScanPlan};

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
            PlanNode::Scan(ScanPlan {
                schema_name: self.db.to_string(),
                table_schema: Arc::new(DataSchema::empty()),
                table_args: Some(ExpressionPlan::Literal(DataValue::Int64(Some(numbers)))),
                projection: None,
                projected_schema: Arc::new(DataSchema::empty()),
                limit: None,
            }),
        )
    }

    pub fn number_source_transform_for_test(&self, numbers: i64) -> Result<SourceTransform> {
        let plan = self.number_read_source_plan_for_test(numbers)?;
        self.ctx.try_set_partitions(plan.partitions)?;
        SourceTransform::try_create(self.ctx.clone(), self.db, self.table)
    }
}
