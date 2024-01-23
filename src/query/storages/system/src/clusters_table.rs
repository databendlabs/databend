// Copyright 2021 Datafuse Labs
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

use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::types::number::NumberScalar;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataBlock;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRefExt;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;

use crate::SyncOneBlockSystemTable;
use crate::SyncSystemTable;

pub struct ClustersTable {
    table_info: TableInfo,
}

impl SyncSystemTable for ClustersTable {
    const NAME: &'static str = "system.cluster";

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    fn get_full_data(&self, ctx: Arc<dyn TableContext>) -> Result<DataBlock> {
        let cluster_nodes = ctx.get_cluster().nodes.clone();

        let mut names = ColumnBuilder::with_capacity(&DataType::String, cluster_nodes.len());
        let mut addresses = ColumnBuilder::with_capacity(&DataType::String, cluster_nodes.len());
        let mut addresses_port = ColumnBuilder::with_capacity(
            &DataType::Number(NumberDataType::UInt16),
            cluster_nodes.len(),
        );
        let mut versions = ColumnBuilder::with_capacity(&DataType::String, cluster_nodes.len());

        for cluster_node in &cluster_nodes {
            let (ip, port) = cluster_node.ip_port()?;

            names.push(Scalar::String(cluster_node.id.clone()).as_ref());
            addresses.push(Scalar::String(ip).as_ref());
            addresses_port.push(Scalar::Number(NumberScalar::UInt16(port)).as_ref());
            versions.push(Scalar::String(cluster_node.binary_version.clone()).as_ref());
        }

        Ok(DataBlock::new_from_columns(vec![
            names.build(),
            addresses.build(),
            addresses_port.build(),
            versions.build(),
        ]))
    }
}

impl ClustersTable {
    pub fn create(table_id: u64) -> Arc<dyn Table> {
        let schema = TableSchemaRefExt::create(vec![
            TableField::new("name", TableDataType::String),
            TableField::new("host", TableDataType::String),
            TableField::new("port", TableDataType::Number(NumberDataType::UInt16)),
            TableField::new("version", TableDataType::String),
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
            ..Default::default()
        };

        SyncOneBlockSystemTable::create(ClustersTable { table_info })
    }
}
