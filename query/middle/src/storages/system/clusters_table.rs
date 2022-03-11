// Copyright 2021 Datafuse Labs.
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

use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::Result;
use common_meta_types::TableIdent;
use common_meta_types::TableInfo;
use common_meta_types::TableMeta;

use crate::sessions::QueryContext;
use crate::storages::system::table::SyncOneBlockSystemTable;
use crate::storages::system::table::SyncSystemTable;
use crate::storages::Table;

pub struct ClustersTable {
    table_info: TableInfo,
}

impl SyncSystemTable for ClustersTable {
    const NAME: &'static str = "system.cluster";

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    fn get_full_data(&self, ctx: Arc<QueryContext>) -> Result<DataBlock> {
        let cluster_nodes = ctx.get_cluster().get_nodes();

        let mut names = MutableStringColumn::with_capacity(cluster_nodes.len());
        let mut addresses = MutableStringColumn::with_capacity(cluster_nodes.len());
        let mut addresses_port = MutablePrimitiveColumn::<u16>::with_capacity(cluster_nodes.len());

        for cluster_node in &cluster_nodes {
            let (ip, port) = cluster_node.ip_port()?;

            names.append_value(cluster_node.id.as_bytes());
            addresses.append_value(ip.as_bytes());
            addresses_port.append_value(port);
        }

        Ok(DataBlock::create(self.table_info.schema(), vec![
            names.finish().arc(),
            addresses.finish().arc(),
            addresses_port.finish().arc(),
        ]))
    }
}

impl ClustersTable {
    pub fn create(table_id: u64) -> Arc<dyn Table> {
        let schema = DataSchemaRefExt::create(vec![
            DataField::new("name", Vu8::to_data_type()),
            DataField::new("host", Vu8::to_data_type()),
            DataField::new("port", u16::to_data_type()),
        ]);

        let table_info = TableInfo {
            desc: "'system'.'clusters'".to_string(),
            name: "clusters".to_string(),
            ident: TableIdent::new(table_id, 0),
            meta: TableMeta {
                schema,
                engine: "SystemClusters".to_string(),
                ..Default::default()
            },
        };

        SyncOneBlockSystemTable::create(ClustersTable { table_info })
    }
}
