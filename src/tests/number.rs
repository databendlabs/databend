// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::sync::Arc;

use crate::datasources::IDataSource;
use crate::datavalues::{DataSchema, DataSchemaRef, DataValue};
use crate::error::FuseQueryResult;
use crate::planners::{ExpressionPlan, PlanNode, ReadDataSourcePlan, ScanPlan};
use crate::sessions::FuseQueryContextRef;
use crate::transforms::SourceTransform;

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

    pub fn number_schema_for_test(&self) -> FuseQueryResult<DataSchemaRef> {
        let datasource = crate::datasources::DataSource::try_create()?;
        let table = datasource.get_table(self.db, self.table)?;
        table.schema()
    }

    pub fn number_read_source_plan_for_test(
        &self,
        numbers: i64,
    ) -> FuseQueryResult<ReadDataSourcePlan> {
        let datasource = crate::datasources::DataSource::try_create()?;
        let table = datasource.get_table(self.db, self.table)?;
        table.read_plan(
            self.ctx.clone(),
            PlanNode::Scan(ScanPlan {
                schema_name: self.db.to_string(),
                table_schema: Arc::new(DataSchema::empty()),
                table_args: Some(ExpressionPlan::Constant(DataValue::Int64(Some(numbers)))),
                projection: None,
                projected_schema: Arc::new(DataSchema::empty()),
            }),
        )
    }

    pub fn number_source_transform_for_test(
        &self,
        numbers: i64,
    ) -> FuseQueryResult<SourceTransform> {
        let plan = self.number_read_source_plan_for_test(numbers)?;
        self.ctx.try_update_partitions(plan.partitions)?;
        SourceTransform::try_create(self.ctx.clone(), self.db, self.table)
    }
}
