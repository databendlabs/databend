// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_datavalues::DataSchema;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataSchemaRefExt;
use common_exception::Result;

use crate::Expression;
use crate::Extras;
use crate::PlanBuilder;
use crate::PlanNode;
use crate::ScanPlan;

pub struct TableScanInfo<'a> {
    pub table_name: &'a str,
    pub table_id: u64,
    pub table_version: Option<u64>,
    pub table_schema: &'a DataSchema,
    pub table_args: Option<Expression>,
}

impl PlanBuilder {
    /// Scan a data source
    pub fn scan(
        schema_name: &str,
        table_scan_info: TableScanInfo,
        projection: Option<Vec<usize>>,
        limit: Option<usize>,
    ) -> Result<PlanBuilder> {
        let table_schema = DataSchemaRef::new(table_scan_info.table_schema.clone());
        let projected_schema = projection.clone().map(|p| {
            DataSchemaRefExt::create(p.iter().map(|i| table_schema.field(*i).clone()).collect())
                .as_ref()
                .clone()
        });
        let projected_schema = match projected_schema {
            None => table_schema.clone(),
            Some(v) => Arc::new(v),
        };

        Ok(PlanBuilder::from(&PlanNode::Scan(ScanPlan {
            schema_name: schema_name.to_owned(),
            table_id: table_scan_info.table_id,
            table_version: table_scan_info.table_version,
            table_schema,
            projected_schema,
            table_args: table_scan_info.table_args,
            push_downs: Extras {
                projection,
                filters: vec![],
                limit,
            },
        })))
    }
}
