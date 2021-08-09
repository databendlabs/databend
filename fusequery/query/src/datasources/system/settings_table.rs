// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::any::Any;
use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::Result;
use common_planners::Part;
use common_planners::ReadDataSourcePlan;
use common_planners::ScanPlan;
use common_planners::Statistics;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;

use crate::datasources::Table;
use crate::sessions::FuseQueryContextRef;

pub struct SettingsTable {
    schema: DataSchemaRef,
}

impl SettingsTable {
    pub fn create() -> Self {
        SettingsTable {
            schema: DataSchemaRefExt::create(vec![
                DataField::new("name", DataType::Utf8, false),
                DataField::new("value", DataType::Utf8, false),
                DataField::new("default_value", DataType::Utf8, false),
                DataField::new("description", DataType::Utf8, false),
            ]),
        }
    }
}

#[async_trait::async_trait]
impl Table for SettingsTable {
    fn name(&self) -> &str {
        "settings"
    }

    fn engine(&self) -> &str {
        "SystemSettings"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Result<DataSchemaRef> {
        Ok(self.schema.clone())
    }

    fn is_local(&self) -> bool {
        true
    }

    fn read_plan(
        &self,
        _ctx: FuseQueryContextRef,
        scan: &ScanPlan,
        _partitions: usize,
    ) -> Result<ReadDataSourcePlan> {
        Ok(ReadDataSourcePlan {
            db: "system".to_string(),
            table: self.name().to_string(),
            table_id: scan.table_id,
            table_version: scan.table_version,
            schema: self.schema.clone(),
            parts: vec![Part {
                name: "".to_string(),
                version: 0,
            }],
            statistics: Statistics::default(),
            description: "(Read from system.settings table)".to_string(),
            scan_plan: Arc::new(scan.clone()),
            remote: false,
        })
    }

    async fn read(
        &self,
        ctx: FuseQueryContextRef,
        _source_plan: &ReadDataSourcePlan,
    ) -> Result<SendableDataBlockStream> {
        let settings = ctx.get_settings();

        let mut names: Vec<String> = vec![];
        let mut values: Vec<String> = vec![];
        let mut default_values: Vec<String> = vec![];
        let mut descs: Vec<String> = vec![];
        for setting in settings.iter() {
            if let DataValue::Struct(vals) = setting {
                names.push(format!("{:?}", vals[0]));
                values.push(format!("{:?}", vals[1]));
                default_values.push(format!("{:?}", vals[2]));
                descs.push(format!("{:?}", vals[3]));
            }
        }

        let names: Vec<&str> = names.iter().map(|x| x.as_str()).collect();
        let values: Vec<&str> = values.iter().map(|x| x.as_str()).collect();
        let default_values: Vec<&str> = default_values.iter().map(|x| x.as_str()).collect();
        let descs: Vec<&str> = descs.iter().map(|x| x.as_str()).collect();
        let block = DataBlock::create_by_array(self.schema.clone(), vec![
            Series::new(names),
            Series::new(values),
            Series::new(default_values),
            Series::new(descs),
        ]);
        Ok(Box::pin(DataBlockStream::create(
            self.schema.clone(),
            None,
            vec![block],
        )))
    }
}
