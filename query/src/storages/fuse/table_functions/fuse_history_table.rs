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
use std::any::TypeId;
use std::sync::Arc;

use common_dal::DataAccessor;
use common_datablocks::DataBlock;
use common_datavalues::prelude::Series;
use common_datavalues::prelude::SeriesFrom;
use common_datavalues::DataField;
use common_datavalues::DataSchemaRefExt;
use common_datavalues::DataType;
use common_datavalues::DataValue;
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
use crate::storages::fuse::io::read_obj;
use crate::storages::fuse::io::snapshot_location;
use crate::storages::fuse::meta::TableSnapshot;
use crate::storages::fuse::FuseTable;
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
            DataField::new("uncompressed_bytes", DataType::UInt64, false),
            DataField::new("compressed_bytes", DataType::UInt64, false),
        ]);

        let (arg_database_name, arg_table_name) = match table_args {
            Some(args) if args.len() == 2 => {
                let db = Self::string_value(&args[0])?;
                let tbl = Self::string_value(&args[1])?;
                Ok((db, tbl))
            }
            _ => Err(ErrorCode::BadArguments(format!(
                "expecting database and table name (as two string literals), but got {:?}",
                table_args
            ))),
        }?;

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

    fn string_value(expr: &Expression) -> Result<String> {
        if let Expression::Literal { value, .. } = expr {
            String::from_utf8(value.as_string()?)
                .map_err(|e| ErrorCode::BadArguments(format!("invalid string. {}", e)))
        } else {
            Err(ErrorCode::BadArguments(format!(
                "expecting string literal, but got {:?}",
                expr
            )))
        }
    }

    fn string_literal(val: &str) -> Expression {
        Expression::create_literal(DataValue::String(Some(val.as_bytes().to_vec())))
    }

    async fn read_snapshots(
        da: &dyn DataAccessor,
        mut location: Option<String>,
    ) -> Result<Vec<TableSnapshot>> {
        let mut snapshots = vec![];
        while let Some(loc) = &location {
            let snapshot: TableSnapshot = read_obj(da, loc).await?;
            let prev = snapshot.prev_snapshot_id;
            snapshots.push(snapshot);
            location = prev.map(|id| snapshot_location(id.to_simple().to_string().as_str()));
        }
        Ok(snapshots)
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

    fn check_table_compatibility(tbl: &dyn Table) -> Result<()> {
        // since StorageFactory is free to choose the engine name,
        // we use type_id to verify the compatibility here
        let tid = tbl.as_any().type_id();
        if tid != TypeId::of::<FuseTable>() {
            Err(ErrorCode::BadArguments(format!(
                "expecting fuse table, but got table of engine type: {}",
                tbl.get_table_info().meta.engine
            )))
        } else {
            Ok(())
        }
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
            Self::string_literal(self.arg_database_name.as_str()),
            Self::string_literal(self.arg_table_name.as_str()),
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

        Self::check_table_compatibility(tbl.as_ref())?;

        let tbl_info = tbl.get_table_info();
        match tbl_info.meta.options.get(TBL_OPT_KEY_SNAPSHOT_LOC) {
            Some(loc) => {
                let da = ctx.get_data_accessor()?;
                let snapshots = Self::read_snapshots(da.as_ref(), Some(loc.clone())).await?;
                let block = self.snapshots_to_block(snapshots);
                Ok::<_, ErrorCode>(vec![block])
            }
            None => Ok(vec![]),
        }
        .map(|blocks| {
            Box::pin(DataBlockStream::create(
                self.table_info.schema(),
                None,
                blocks,
            )) as SendableDataBlockStream
        })
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
