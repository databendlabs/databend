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

use databend_common_catalog::catalog_kind::CATALOG_DEFAULT;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::table_args::TableArgs;
use databend_common_catalog::table_context::TableContext;
use databend_common_expression::DataBlock;
use databend_common_expression::TableSchemaRef;
use databend_storages_common_table_meta::meta::TableSnapshot;
use futures_util::TryStreamExt;

use crate::io::MetaReaders;
use crate::io::SnapshotHistoryReader;
use crate::table_functions::parse_db_tb_opt_args;
use crate::table_functions::string_literal;
use crate::table_functions::FuseColumn;
use crate::table_functions::SimpleTableFunc;
use crate::FuseTable;

pub struct CommonArgs {
    pub arg_database_name: String,
    pub arg_table_name: String,
    pub arg_snapshot_id: Option<String>,
}

impl From<&CommonArgs> for TableArgs {
    fn from(value: &CommonArgs) -> Self {
        let mut args = Vec::new();
        args.push(string_literal(value.arg_database_name.as_str()));
        args.push(string_literal(value.arg_table_name.as_str()));
        if let Some(arg_snapshot_id) = &value.arg_snapshot_id {
            args.push(string_literal(arg_snapshot_id));
        }
        TableArgs::new_positioned(args)
    }
}

pub async fn location_snapshot(
    tbl: &FuseTable,
    args: &CommonArgs,
) -> databend_common_exception::Result<Option<Arc<TableSnapshot>>> {
    let snapshot_id = args.arg_snapshot_id.clone();
    let maybe_snapshot = tbl.read_table_snapshot().await?;
    if let Some(snapshot) = maybe_snapshot {
        if let Some(snapshot_id) = snapshot_id {
            // prepare the stream of snapshot
            let snapshot_version = tbl.snapshot_format_version(None).await?;
            let snapshot_location = tbl
                .meta_location_generator
                .snapshot_location_from_uuid(&snapshot.snapshot_id, snapshot_version)?;
            let reader = MetaReaders::table_snapshot_reader(tbl.get_operator());
            let mut snapshot_stream = reader.snapshot_history(
                snapshot_location,
                snapshot_version,
                tbl.meta_location_generator().clone(),
            );

            // find the element by snapshot_id in stream
            while let Some((snapshot, _)) = snapshot_stream.try_next().await? {
                if snapshot.snapshot_id.simple().to_string() == snapshot_id {
                    return Ok(Some(snapshot));
                }
            }
        } else {
            return Ok(Some(snapshot));
        }
    }

    Ok(None)
}

#[async_trait::async_trait]
pub trait CommonArgFunction {
    async fn apply(
        &self,
        ctx: &Arc<dyn TableContext>,
        tbl: &FuseTable,
        snapshot: Arc<TableSnapshot>,
        limit: Option<usize>,
    ) -> databend_common_exception::Result<DataBlock>;
}

impl<T> SimpleTableFunc for T
where T: CommonArgFunction + Send + Sync
{
    fn table_args(&self) -> Option<TableArgs> {
        // Some((&self.args).into())
        todo!()
    }
    fn schema(&self) -> TableSchemaRef {
        // Self::schema()
        todo!()
    }

    async fn apply(
        &self,
        ctx: &Arc<dyn TableContext>,
        plan: &DataSourcePlan,
    ) -> databend_common_exception::Result<Option<DataBlock>> {
        todo!()
        // let tenant_id = ctx.get_tenant();
        // let tbl = ctx
        //    .get_catalog(CATALOG_DEFAULT)
        //    .await?
        //    .get_table(
        //        &tenant_id,
        //        self.args.arg_database_name.as_str(),
        //        self.args.arg_table_name.as_str(),
        //    )
        //    .await?;
        // let limit = plan.push_downs.as_ref().and_then(|x| x.limit);
        // let tbl = FuseTable::try_from_table(tbl.as_ref())?;
        // Ok(Some(self.get_blocks(ctx, tbl, limit).await?))
    }

    fn create(table_args: TableArgs) -> databend_common_exception::Result<Self>
    where Self: Sized {
        todo!()
        // let (arg_database_name, arg_table_name, arg_snapshot_id) = parse_db_tb_opt_args(
        //    &table_args,
        //    crate::table_functions::fuse_columns::fuse_column::FUSE_FUNC_COLUMN,
        //)?;
        // Ok(Self {
        //    args: CommonArgs {
        //        arg_database_name,
        //        arg_table_name,
        //        arg_snapshot_id,
        //    },
        //})
    }
}
