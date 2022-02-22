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
//

use std::any::Any;
use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_types::TableIdent;
use common_meta_types::TableInfo;
use common_meta_types::TableMeta;
use common_planners::Expression;
use common_planners::ReadDataSourcePlan;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;

use crate::catalogs::Catalog;
use crate::sessions::QueryContext;
use crate::storages::fuse::io::MetaReaders;
use crate::storages::fuse::meta::TableSnapshot;
use crate::storages::fuse::table::is_fuse_table;
use crate::storages::fuse::table_functions::table_arg_util::parse_func_history_args;
use crate::storages::fuse::table_functions::table_arg_util::string_literal;
use crate::storages::fuse::TBL_OPT_KEY_SNAPSHOT_LOC;
use crate::storages::Table;
use crate::table_functions::TableArgs;
use crate::table_functions::TableFunction;

pub const FUSE_FUNC_HIST: &str = "fuse_history";

pub struct FuseHistoryTable {
    table_info: TableInfo,
    arg_database_name: String,
    arg_table_name: String,
}

impl FuseHistoryTable {
    pub fn create(
        database_name: &str,
        table_func_name: &str,
        table_id: u64,
        table_args: TableArgs,
    ) -> Result<Arc<dyn TableFunction>> {
        let schema = DataSchemaRefExt::create(vec![
            DataField::new("snapshot_id", Vu8::to_data_type()),
            DataField::new_nullable("prev_snapshot_id", Vu8::to_data_type()),
            DataField::new("segment_count", u64::to_data_type()),
            DataField::new("block_count", u64::to_data_type()),
            DataField::new("row_count", u64::to_data_type()),
            DataField::new("bytes_uncompressed", u64::to_data_type()),
            DataField::new("bytes_compressed", u64::to_data_type()),
        ]);

        let (arg_database_name, arg_table_name) = parse_func_history_args(&table_args)?;

        let engine = FUSE_FUNC_HIST.to_owned();

        let table_info = TableInfo {
            ident: TableIdent::new(table_id, 0),
            desc: format!("'{}'.'{}'", database_name, table_func_name),
            name: table_func_name.to_string(),
            meta: TableMeta {
                schema,
                engine,
                ..Default::default()
            },
        };

        Ok(Arc::new(FuseHistoryTable {
            table_info,
            arg_database_name,
            arg_table_name,
        }))
    }

    fn snapshots_to_block(&self, snapshots: Vec<Arc<TableSnapshot>>) -> DataBlock {
        let len = snapshots.len();
        let mut snapshot_ids: Vec<Vec<u8>> = Vec::with_capacity(len);
        let mut prev_snapshot_ids: Vec<Option<Vec<u8>>> = Vec::with_capacity(len);
        let mut segment_count: Vec<u64> = Vec::with_capacity(len);
        let mut block_count: Vec<u64> = Vec::with_capacity(len);
        let mut row_count: Vec<u64> = Vec::with_capacity(len);
        let mut compressed: Vec<u64> = Vec::with_capacity(len);
        let mut uncompressed: Vec<u64> = Vec::with_capacity(len);
        for s in snapshots {
            snapshot_ids.push(s.snapshot_id.simple().to_string().into_bytes());
            prev_snapshot_ids.push(
                s.prev_snapshot_id
                    .map(|v| v.simple().to_string().into_bytes()),
            );
            segment_count.push(s.segments.len() as u64);
            block_count.push(s.summary.block_count);
            row_count.push(s.summary.row_count);
            compressed.push(s.summary.compressed_byte_size);
            uncompressed.push(s.summary.uncompressed_byte_size);
        }

        DataBlock::create(self.table_info.schema(), vec![
            Series::from_data(snapshot_ids),
            Series::from_data(prev_snapshot_ids),
            Series::from_data(segment_count),
            Series::from_data(block_count),
            Series::from_data(row_count),
            Series::from_data(uncompressed),
            Series::from_data(compressed),
        ])
    }
}

#[async_trait::async_trait]
impl Table for FuseHistoryTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    fn table_args(&self) -> Option<Vec<Expression>> {
        Some(vec![
            string_literal(self.arg_database_name.as_str()),
            string_literal(self.arg_table_name.as_str()),
        ])
    }

    async fn read(
        &self,
        ctx: Arc<QueryContext>,
        _plan: &ReadDataSourcePlan,
    ) -> Result<SendableDataBlockStream> {
        let tenant_id = ctx.get_tenant();
        let tbl = ctx
            .get_catalog()
            .get_table(
                tenant_id.as_str(),
                self.arg_database_name.as_str(),
                self.arg_table_name.as_str(),
            )
            .await?;

        if !is_fuse_table(tbl.as_ref()) {
            return Err(ErrorCode::BadArguments(format!(
                "expecting fuse table, but got table of engine type: {}",
                tbl.get_table_info().meta.engine
            )));
        }

        let tbl_info = tbl.get_table_info();
        let location = tbl_info.meta.options.get(TBL_OPT_KEY_SNAPSHOT_LOC);
        let reader = MetaReaders::table_snapshot_reader(ctx.as_ref());
        let snapshots = reader.read_snapshot_history(location).await?;
        let blocks = vec![self.snapshots_to_block(snapshots)];
        Ok(Box::pin(DataBlockStream::create(
            self.table_info.schema(),
            None,
            blocks,
        )))
    }
}

impl TableFunction for FuseHistoryTable {
    fn function_name(&self) -> &str {
        self.name()
    }

    fn as_table<'a>(self: Arc<Self>) -> Arc<dyn Table + 'a>
    where Self: 'a {
        self
    }
}
