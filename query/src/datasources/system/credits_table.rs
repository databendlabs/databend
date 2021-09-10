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

use crate::catalogs::Table;
use crate::sessions::DatafuseQueryContextRef;

pub struct CreditsTable {
    schema: DataSchemaRef,
}

impl CreditsTable {
    pub fn create() -> Self {
        CreditsTable {
            schema: DataSchemaRefExt::create(vec![
                DataField::new("name", DataType::String, false),
                DataField::new("version", DataType::String, false),
                DataField::new("license", DataType::String, false),
            ]),
        }
    }
}

#[async_trait::async_trait]
impl Table for CreditsTable {
    fn name(&self) -> &str {
        "credits"
    }

    fn engine(&self) -> &str {
        "SystemCredits"
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
        _ctx: DatafuseQueryContextRef,
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
            description: "(Read from system.credits table)".to_string(),
            scan_plan: Arc::new(scan.clone()),
            remote: false,
        })
    }

    async fn read(
        &self,
        _ctx: DatafuseQueryContextRef,
        _source_plan: &ReadDataSourcePlan,
    ) -> Result<SendableDataBlockStream> {
        let metadata_command = cargo_metadata::MetadataCommand::new();

        let deps =
            match cargo_license::get_dependencies_from_cargo_lock(metadata_command, false, false) {
                Ok(v) => v,
                Err(err) => {
                    log::error!("{:?}", err);
                    vec![]
                }
            };

        let names: Vec<&[u8]> = deps.iter().map(|x| x.name.as_bytes()).collect();
        let version_strings: Vec<String> = deps.iter().map(|x| x.version.to_string()).collect();
        let versions: Vec<&[u8]> = version_strings.iter().map(|x| x.as_bytes()).collect();
        let licenses: Vec<&[u8]> = deps
            .iter()
            .map(|x| match &x.license {
                None => b"UNKNOWN",
                Some(license) => license.as_bytes(),
            })
            .collect();

        let block = DataBlock::create_by_array(self.schema.clone(), vec![
            Series::new(names),
            Series::new(versions),
            Series::new(licenses),
        ]);

        Ok(Box::pin(DataBlockStream::create(
            self.schema.clone(),
            None,
            vec![block],
        )))
    }
}
