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
use crate::storages::fuse::TBL_OPT_KEY_SNAPSHOT_LOC;
use crate::storages::Table;
use crate::table_functions::TableArgs;
use crate::table_functions::TableFunction;

pub struct HistoriesTable {
    table_info: TableInfo,
    arg_database_name: String,
    arg_table_name: String,
}

impl HistoriesTable {
    fn string_value(expr: &Expression) -> Result<String> {
        if let Expression::Literal { value, .. } = expr {
            String::from_utf8(value.as_string()?)
                .map_err(|e| ErrorCode::BadArguments(format!("invalid string. {}", e.to_string())))
        } else {
            Err(ErrorCode::BadArguments(format!(
                "expecting string literal, but got {:?}",
                expr
            )))
        }
    }
    pub fn create(
        database_name: &str,
        table_func_name: &str,
        table_id: u64,
        table_args: TableArgs,
    ) -> Result<Arc<dyn TableFunction>> {
        let schema = DataSchemaRefExt::create(vec![
            DataField::new("snapshot_id", DataType::String, false),
            DataField::new("prev_snapshot_id", DataType::String, true),
            DataField::new("row_count", DataType::UInt64, false),
            DataField::new("block_count", DataType::UInt64, false),
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
                "expecting two string literals, but got {:?}",
                table_args
            ))),
        }?;

        let engine = "fuse_hist".to_owned();

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

        Ok(Arc::new(HistoriesTable {
            table_info,
            arg_database_name,
            arg_table_name,
        }))
    }

    async fn read_snapshots(
        da: &dyn DataAccessor,
        mut location: Option<String>,
        snapshots: &mut Vec<TableSnapshot>,
    ) -> Result<()> {
        while let Some(loc) = &location {
            let snapshot: TableSnapshot = read_obj(da, loc).await?;
            let prev = snapshot.prev_snapshot_id.clone();
            snapshots.push(snapshot);
            location = prev.map(|id| snapshot_location(id.to_simple().to_string().as_str()));
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl Table for HistoriesTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    fn table_args(&self) -> Option<Vec<Expression>> {
        Some(vec![
            Expression::create_literal(DataValue::String(Some(
                self.arg_database_name.as_bytes().to_vec(),
            ))),
            Expression::create_literal(DataValue::String(Some(
                self.arg_table_name.as_bytes().to_vec(),
            ))),
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
        let tbl_info = tbl.get_table_info();
        let blocks = if let Some(loc) = tbl_info.meta.options.get(TBL_OPT_KEY_SNAPSHOT_LOC) {
            let da = ctx.get_data_accessor()?;
            let mut snapshots = vec![];
            Self::read_snapshots(da.as_ref(), Some(loc.clone()), &mut snapshots).await?;
            let snapshot_ids: Vec<Vec<u8>> = snapshots
                .iter()
                .map(|s| s.snapshot_id.to_simple().to_string().into_bytes())
                .collect();

            let prev_snapshot_ids: Vec<Option<Vec<u8>>> = snapshots
                .iter()
                .map(|s| {
                    s.prev_snapshot_id
                        .map(|ref p| p.to_simple().to_string().into_bytes())
                })
                .collect();

            let row_count: Vec<u64> = snapshots.iter().map(|s| s.summary.row_count).collect();
            let block_count: Vec<u64> = snapshots.iter().map(|s| s.summary.block_count).collect();
            let uncompressed: Vec<u64> = snapshots
                .iter()
                .map(|s| s.summary.uncompressed_byte_size)
                .collect();
            let compressed: Vec<u64> = snapshots
                .iter()
                .map(|s| s.summary.compressed_byte_size)
                .collect();

            let block = DataBlock::create_by_array(self.table_info.schema(), vec![
                Series::new(snapshot_ids),
                Series::new(prev_snapshot_ids),
                Series::new(row_count),
                Series::new(block_count),
                Series::new(uncompressed),
                Series::new(compressed),
            ]);
            Ok::<_, ErrorCode>(vec![block])
        } else {
            Ok(vec![])
        }?;
        Ok(Box::pin(DataBlockStream::create(
            self.table_info.schema(),
            None,
            blocks,
        )))
    }
}

impl TableFunction for HistoriesTable {
    fn function_name(&self) -> &str {
        self.name()
    }

    fn as_table<'a>(self: Arc<Self>) -> Arc<dyn Table + 'a>
    where Self: 'a {
        self
    }
}
