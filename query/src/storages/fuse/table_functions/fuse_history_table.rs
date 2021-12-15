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
use common_datavalues::prelude::Series;
use common_datavalues::prelude::SeriesFrom;
use common_datavalues::DataField;
use common_datavalues::DataSchemaRefExt;
use common_datavalues::DataType;
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
use crate::storages::fuse::io::snapshot_history;
use crate::storages::fuse::meta::TableSnapshot;
use crate::storages::fuse::table::check_table_compatibility;
use crate::storages::fuse::table_functions::table_arg_util::parse_table_history_args;
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
            DataField::new("snapshot_id", DataType::String, false),
            DataField::new("prev_snapshot_id", DataType::String, true),
            DataField::new("segment_count", DataType::UInt64, false),
            DataField::new("block_count", DataType::UInt64, false),
            DataField::new("row_count", DataType::UInt64, false),
            DataField::new("bytes_uncompressed", DataType::UInt64, false),
            DataField::new("bytes_compressed", DataType::UInt64, false),
        ]);

        let (arg_database_name, arg_table_name) = parse_table_history_args(&table_args)?;

        let engine = FUSE_FUNC_HIST.to_owned();

        let table_info = TableInfo {
            ident: TableIdent::new(table_id, 0),
            desc: format!("'{}'.'{}'", database_name, table_func_name),
            name: table_func_name.to_string(),
            meta: TableMeta {
                schema,
                engine,
                options: Default::default(),
            },
        };

        Ok(Arc::new(FuseHistoryTable {
            table_info,
            arg_database_name,
            arg_table_name,
        }))
    }

    fn snapshots_to_block(&self, snapshots: Vec<TableSnapshot>) -> DataBlock {
        let len = snapshots.len();
        let mut snapshot_ids: Vec<Vec<u8>> = Vec::with_capacity(len);
        let mut prev_snapshot_ids: Vec<Option<Vec<u8>>> = Vec::with_capacity(len);
        let mut segment_count: Vec<u64> = Vec::with_capacity(len);
        let mut block_count: Vec<u64> = Vec::with_capacity(len);
        let mut row_count: Vec<u64> = Vec::with_capacity(len);
        let mut compressed: Vec<u64> = Vec::with_capacity(len);
        let mut uncompressed: Vec<u64> = Vec::with_capacity(len);
        for s in snapshots {
            snapshot_ids.push(s.snapshot_id.to_simple().to_string().into_bytes());
            prev_snapshot_ids.push(
                s.prev_snapshot_id
                    .map(|v| v.to_simple().to_string().into_bytes()),
            );
            segment_count.push(s.segments.len() as u64);
            block_count.push(s.summary.block_count);
            row_count.push(s.summary.row_count);
            compressed.push(s.summary.compressed_byte_size);
            uncompressed.push(s.summary.uncompressed_byte_size);
        }

        DataBlock::create_by_array(self.table_info.schema(), vec![
            Series::new(snapshot_ids),
            Series::new(prev_snapshot_ids),
            Series::new(segment_count),
            Series::new(block_count),
            Series::new(row_count),
            Series::new(uncompressed),
            Series::new(compressed),
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
        let tbl = ctx
            .get_catalog()
            .get_table(
                self.arg_database_name.as_str(),
                self.arg_table_name.as_str(),
            )
            .await?;

        check_table_compatibility(tbl.as_ref())?;
        let tbl_info = tbl.get_table_info();
        let location = tbl_info.meta.options.get(TBL_OPT_KEY_SNAPSHOT_LOC);
        let da = ctx.get_data_accessor()?;
        let snapshots = snapshot_history(da.as_ref(), location).await?;
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
