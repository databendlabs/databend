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
use std::collections::HashSet;
use std::sync::Arc;

use common_dal::DataAccessor;
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
use crate::storages::fuse::io;
use crate::storages::fuse::io::read_obj;
use crate::storages::fuse::io::snapshot_location;
use crate::storages::fuse::meta::SegmentInfo;
use crate::storages::fuse::meta::TableSnapshot;
use crate::storages::fuse::FuseTable;
use crate::storages::fuse::TBL_OPT_KEY_SNAPSHOT_LOC;
use crate::storages::Table;
use crate::table_functions::TableArgs;
use crate::table_functions::TableFunction;

pub const FUSE_FUNC_TRUNCATE: &str = "fuse_truncate_history";

// TODO duplicated codes

pub struct FuseTruncateHistory {
    table_info: TableInfo,
    arg_database_name: String,
    arg_table_name: String,
}

impl FuseTruncateHistory {
    pub(crate) async fn remove_location(
        &self,
        _da: Arc<dyn DataAccessor>,
        loc: impl AsRef<str>,
    ) -> Result<()> {
        let str = loc.as_ref();
        eprintln!(">>>> DELETE LOC {}", str);
        Ok(())
    }
}

impl FuseTruncateHistory {
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

        let engine = FUSE_FUNC_TRUNCATE.to_owned();

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

        Ok(Arc::new(FuseTruncateHistory {
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

    fn empty_result(&self) -> Result<SendableDataBlockStream> {
        Ok(Box::pin(DataBlockStream::create(
            self.table_info.schema(),
            None,
            vec![],
        )))
    }

    async fn blocks_of(
        &self,
        da: Arc<dyn DataAccessor>,
        locs: impl Iterator<Item = impl AsRef<str>>,
    ) -> Result<HashSet<String>> {
        let mut r = HashSet::new();
        for x in locs {
            let res: SegmentInfo = io::read_obj(da.as_ref(), x).await?;
            for x in res.blocks {
                r.insert(x.location.location);
            }
        }
        Ok(r)
    }
}

#[async_trait::async_trait]
impl Table for FuseTruncateHistory {
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
        let mut snapshots = match tbl_info.meta.options.get(TBL_OPT_KEY_SNAPSHOT_LOC) {
            Some(loc) => {
                let da = ctx.get_data_accessor()?;
                let snapshots = Self::read_snapshots(da.as_ref(), Some(loc.clone())).await?;
                Ok::<_, ErrorCode>(snapshots)
            }
            None => Ok(vec![]),
        }?;

        // short cut
        if snapshots.len() <= 1 {
            return self.empty_result();
        }

        let current_snapshot = snapshots.remove(0);
        let current_segments: HashSet<&String> = HashSet::from_iter(&current_snapshot.segments);
        let prevs = snapshots.iter().fold(HashSet::new(), |mut acc, s| {
            acc.extend(&s.segments);
            acc
        });

        // segments which no longer need to be kept
        let seg_delta = prevs.difference(&current_segments).collect::<Vec<_>>();

        let da = ctx.get_data_accessor()?;
        // blocks to be removed
        let prev_blocks: HashSet<String> = self.blocks_of(da.clone(), seg_delta.iter()).await?;
        let current_blocks: HashSet<String> = self
            .blocks_of(da.clone(), current_snapshot.segments.iter())
            .await?;

        // blocks that can be
        let block_delta = prev_blocks.difference(&current_blocks);

        // 1. remove blocks
        for x in block_delta {
            self.remove_location(da.clone(), x).await?
        }

        // 2. remove the segments
        for x in seg_delta {
            self.remove_location(da.clone(), x).await?
        }

        // 3. remove the blocks
        for x in snapshots.iter().rev() {
            let loc = snapshot_location(x.snapshot_id.to_simple().to_string());
            self.remove_location(da.clone(), loc).await?
        }

        self.empty_result()
    }
}

impl TableFunction for FuseTruncateHistory {
    fn function_name(&self) -> &str {
        self.name()
    }

    fn as_table<'a>(self: Arc<Self>) -> Arc<dyn Table + 'a>
    where Self: 'a {
        self
    }
}
