// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::sync::{Arc, Mutex};

use crate::contexts::FuseQueryContext;
use crate::datasources::IDataSource;
use crate::datavalues::{DataSchema, DataSchemaRef, DataValue};
use crate::error::FuseQueryResult;
use crate::planners::{ExpressionPlan, PlanNode, ReadDataSourcePlan, ScanPlan};
use crate::transforms::SourceTransform;

pub struct NumberTestData {
    db: &'static str,
    table: &'static str,
}

impl NumberTestData {
    pub fn create() -> Self {
        NumberTestData {
            db: "system",
            table: "numbers",
        }
    }

    pub fn number_schema_for_test(&self) -> FuseQueryResult<DataSchemaRef> {
        let datasource = crate::datasources::DataSource::try_create()?;
        let table = datasource.get_table(self.db, self.table)?;
        table.schema()
    }

    pub fn number_source_for_test(&self) -> FuseQueryResult<Arc<Mutex<dyn IDataSource>>> {
        Ok(Arc::new(Mutex::new(
            crate::datasources::DataSource::try_create()?,
        )))
    }

    pub fn number_read_source_plan_for_test(
        &self,
        numbers: i64,
    ) -> FuseQueryResult<ReadDataSourcePlan> {
        let datasource = crate::datasources::DataSource::try_create()?;
        let table = datasource.get_table(self.db, self.table)?;
        table.read_plan(PlanNode::Scan(ScanPlan {
            schema_name: self.db.to_string(),
            table_schema: Arc::new(DataSchema::empty()),
            table_args: Some(ExpressionPlan::Constant(DataValue::Int64(Some(numbers)))),
            projection: None,
            projected_schema: Arc::new(DataSchema::empty()),
        }))
    }

    pub fn number_source_transform_for_test(
        &self,
        numbers: i64,
    ) -> FuseQueryResult<SourceTransform> {
        let datasource = crate::datasources::DataSource::try_create()?;
        let table = datasource.get_table(self.db, self.table)?;
        let plan = table.read_plan(PlanNode::Scan(ScanPlan {
            schema_name: self.db.to_string(),
            table_schema: Arc::new(DataSchema::empty()),
            table_args: Some(ExpressionPlan::Constant(DataValue::Int64(Some(numbers)))),
            projection: None,
            projected_schema: Arc::new(DataSchema::empty()),
        }))?;
        let ctx = FuseQueryContext::create_ctx(0, Arc::new(Mutex::new(datasource)));

        SourceTransform::try_create(Arc::new(ctx), self.db, self.table, plan.partitions)
    }
}
