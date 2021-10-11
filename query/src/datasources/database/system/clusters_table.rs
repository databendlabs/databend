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

use common_context::IOContext;
use common_context::TableIOContext;
use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::Result;
use common_meta_types::TableInfo;
use common_planners::Extras;
use common_planners::Part;
use common_planners::ReadDataSourcePlan;
use common_planners::Statistics;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;

use crate::catalogs::Table;

pub struct ClustersTable {
    table_info: TableInfo,
}

impl ClustersTable {
    pub fn create(table_id: u64) -> Self {
        let schema = DataSchemaRefExt::create(vec![
            DataField::new("name", DataType::String, false),
            DataField::new("host", DataType::String, false),
            DataField::new("port", DataType::UInt16, false),
        ]);

        let table_info = TableInfo {
            db: "system".to_string(),
            name: "clusters".to_string(),
            table_id,
            schema,
            engine: "SystemClusters".to_string(),
            is_local: true,
            ..Default::default()
        };

        ClustersTable { table_info }
    }
}

#[async_trait::async_trait]
impl Table for ClustersTable {
    fn name(&self) -> &str {
        &self.table_info.name
    }

    fn engine(&self) -> &str {
        &self.table_info.engine
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Result<DataSchemaRef> {
        Ok(self.table_info.schema.clone())
    }

    fn get_id(&self) -> u64 {
        self.table_info.table_id
    }

    fn is_local(&self) -> bool {
        self.table_info.is_local
    }

    fn read_plan(
        &self,
        _io_ctx: Arc<TableIOContext>,
        _push_downs: Option<Extras>,
        _partition_num_hint: Option<usize>,
    ) -> Result<ReadDataSourcePlan> {
        Ok(ReadDataSourcePlan {
            table_info: self.table_info.clone(),
            parts: vec![Part {
                name: "".to_string(),
                version: 0,
            }],
            statistics: Statistics::default(),
            description: "(Read from system.clusters table)".to_string(),
            scan_plan: Default::default(), // scan_plan will be removed form ReadSourcePlan soon
            tbl_args: None,
            push_downs: None,
        })
    }

    async fn read(
        &self,
        io_ctx: Arc<TableIOContext>,
        _push_downs: &Option<Extras>,
    ) -> Result<SendableDataBlockStream> {
        let cluster_nodes = io_ctx.get_query_nodes();

        let mut names = StringArrayBuilder::with_capacity(cluster_nodes.len());
        let mut addresses = StringArrayBuilder::with_capacity(cluster_nodes.len());
        let mut addresses_port = DFUInt16ArrayBuilder::with_capacity(cluster_nodes.len());

        for cluster_node in &cluster_nodes {
            let (ip, port) = cluster_node.ip_port()?;

            names.append_value(cluster_node.id.as_bytes());
            addresses.append_value(ip.as_bytes());
            addresses_port.append_value(port);
        }

        Ok(Box::pin(DataBlockStream::create(
            self.table_info.schema.clone(),
            None,
            vec![DataBlock::create_by_array(
                self.table_info.schema.clone(),
                vec![
                    names.finish().into_series(),
                    addresses.finish().into_series(),
                    addresses_port.finish().into_series(),
                ],
            )],
        )))
    }
}
