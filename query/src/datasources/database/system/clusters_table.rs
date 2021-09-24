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
use std::net::SocketAddr;
use std::str::FromStr;
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
use crate::sessions::DatabendQueryContextRef;

pub struct ClustersTable {
    schema: DataSchemaRef,
}

impl ClustersTable {
    pub fn create() -> Self {
        ClustersTable {
            schema: DataSchemaRefExt::create(vec![
                DataField::new("name", DataType::String, false),
                DataField::new("host", DataType::String, false),
                DataField::new("port", DataType::UInt16, false),
                // DataField::new("priority", DataType::UInt8, false),
            ]),
        }
    }
}

#[async_trait::async_trait]
impl Table for ClustersTable {
    fn name(&self) -> &str {
        "clusters"
    }

    fn engine(&self) -> &str {
        "SystemClusters"
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
        _ctx: DatabendQueryContextRef,
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
            description: "(Read from system.clusters table)".to_string(),
            scan_plan: Arc::new(scan.clone()),
            remote: false,
        })
    }

    async fn read(
        &self,
        ctx: DatabendQueryContextRef,
        _source_plan: &ReadDataSourcePlan,
    ) -> Result<SendableDataBlockStream> {
        let cluster = ctx.get_cluster();
        let cluster_nodes = cluster.get_nodes();

        let mut names = StringArrayBuilder::with_capacity(cluster_nodes.len());
        let mut addresses = StringArrayBuilder::with_capacity(cluster_nodes.len());
        let mut addresses_port = DFUInt16ArrayBuilder::with_capacity(cluster_nodes.len());

        for cluster_node in &cluster_nodes {
            let address = SocketAddr::from_str(&cluster_node.flight_address)?;

            names.append_value(cluster_node.id.as_bytes());
            addresses.append_value(address.ip().to_string().as_bytes());
            addresses_port.append_value(address.port());
        }

        Ok(Box::pin(DataBlockStream::create(
            self.schema.clone(),
            None,
            vec![DataBlock::create_by_array(self.schema.clone(), vec![
                names.finish().into_series(),
                addresses.finish().into_series(),
                addresses_port.finish().into_series(),
            ])],
        )))
    }
}
