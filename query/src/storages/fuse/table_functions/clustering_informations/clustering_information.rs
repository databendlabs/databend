//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::Result;
use common_planners::Expression;

use crate::sessions::QueryContext;
use crate::storages::fuse::io::BlockReader;
use crate::storages::fuse::io::MetaReaders;
use crate::storages::fuse::FuseTable;
use crate::storages::Table;
use crate::storages::index::ClusteringInformationExecutor;

pub struct ClusteringInformation<'a> {
    pub ctx: Arc<QueryContext>,
    pub table: &'a FuseTable,
    pub cluster_keys: Vec<Expression>,
}

impl<'a> ClusteringInformation<'a> {
    pub fn new(
        ctx: Arc<QueryContext>,
        table: &'a FuseTable,
        cluster_keys: Vec<Expression>,
    ) -> Self {
        Self {
            ctx,
            table,
            cluster_keys,
        }
    }

    pub async fn get_clustering_info(&self) -> Result<DataBlock> {
        let snapshot = self.table.read_table_snapshot(self.ctx.as_ref()).await?;

        let mut blocks = Vec::new();
        if let Some(snapshot) = snapshot {
            let reader = MetaReaders::segment_info_reader(self.ctx.as_ref());
            for (x, ver) in &snapshot.segments {
                let res = reader.read(x, None, *ver).await?;
                let mut block = res.blocks.clone();
                blocks.append(&mut block);
            }
        };

        if self.table.cluster_keys() != self.cluster_keys {
            todo!()
        }

        let names = self.cluster_keys.iter().map(|x| x.column_name()).collect().join(", ");
        let cluster_by_keys = format!("({})", names);
        let executor = ClusteringInformationExecutor::create_by_cluster(blocks)?;
        let info = executor.execute()?;

        Ok(DataBlock::create(ClusteringInformation::schema(), vec![
            Series::from_data(vec![cluster_by_keys]),
            Series::from_data(vec![info.total_block_count]),
            Series::from_data(vec![info.total_constant_block_count]),
            Series::from_data(vec![info.average_overlaps]),
            Series::from_data(vec![info.average_depth]),
            Series::from_data(vec![info.block_depth_histogram]),
        ]))
    }

    pub fn schema() -> Arc<DataSchema> {
        DataSchemaRefExt::create(vec![
            DataField::new("cluster_keys", Vu8::to_data_type()),
            DataField::new("total_block_count", u64::to_data_type()),
            DataField::new("total_constant_block_count", u64::to_data_type()),
            DataField::new("average_overlaps", f64::to_data_type()),
            DataField::new("average_depth", f64::to_data_type()),
            DataField::new("block_depth_histogram", VariantArrayType::new_impl()),
        ])
    }
}
