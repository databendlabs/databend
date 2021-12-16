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
use std::collections::HashSet;
use std::sync::Arc;

use common_dal::DataAccessor;
use common_datablocks::DataBlock;
use common_datavalues::chrono::Utc;
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
use crate::storages::fuse::io;
use crate::storages::fuse::io::snapshot_history;
use crate::storages::fuse::io::snapshot_location;
use crate::storages::fuse::meta::SegmentInfo;
use crate::storages::fuse::meta::TableSnapshot;
use crate::storages::fuse::table::is_fuse_table;
use crate::storages::fuse::table_functions::table_arg_util::parse_func_truncate_history_args;
use crate::storages::fuse::table_functions::table_arg_util::string_literal;
use crate::storages::fuse::TBL_OPT_KEY_SNAPSHOT_LOC;
use crate::storages::Table;
use crate::table_functions::TableArgs;
use crate::table_functions::TableFunction;

pub const FUSE_FUNC_TRUNCATE: &str = "fuse_truncate_history";

pub struct FuseTruncateHistory {
    table_info: TableInfo,
    arg_database_name: String,
    arg_table_name: String,
    truncate_all: bool,
}

impl FuseTruncateHistory {
    pub fn new(
        database_name: &str,
        table_func_name: &str,
        table_id: u64,
        arg_database_name: String,
        arg_table_name: String,
        truncate_all: bool,
    ) -> Arc<FuseTruncateHistory> {
        let schema = DataSchemaRefExt::create(vec![
            DataField::new("snapshot_removed", DataType::UInt64, false),
            DataField::new("segment_removed", DataType::UInt64, false),
            DataField::new("block_removed", DataType::UInt64, false),
        ]);
        let engine = FUSE_FUNC_TRUNCATE.to_owned();
        let table_info = TableInfo {
            ident: TableIdent::new(table_id, 0),
            desc: format!("'{}'.'{}'", database_name, table_func_name),
            name: table_func_name.to_string(),
            meta: TableMeta {
                schema,
                engine,
                options: Default::default(),
                created_on: Utc::now(),
            },
        };
        Arc::new(FuseTruncateHistory {
            table_info,
            arg_database_name,
            arg_table_name,
            truncate_all,
        })
    }

    pub fn create(
        database_name: &str,
        table_func_name: &str,
        table_id: u64,
        table_args: TableArgs,
    ) -> Result<Arc<dyn TableFunction>> {
        let (arg_database_name, arg_table_name, all) =
            parse_func_truncate_history_args(&table_args)?;
        Ok(Self::new(
            database_name,
            table_func_name,
            table_id,
            arg_database_name,
            arg_table_name,
            all,
        ))
    }

    pub async fn truncate_history(
        &self,
        ctx: Arc<QueryContext>,
    ) -> Result<Option<(u64, u64, u64)>> {
        let tbl = ctx
            .get_catalog()
            .get_table(
                self.arg_database_name.as_str(),
                self.arg_table_name.as_str(),
            )
            .await?;

        if !is_fuse_table(tbl.as_ref()) {
            return Ok(None);
        }

        let da = ctx.get_data_accessor()?;
        let tbl_info = tbl.get_table_info();
        let snapshot_loc = tbl_info.meta.options.get(TBL_OPT_KEY_SNAPSHOT_LOC);
        let mut snapshots = snapshot_history(da.as_ref(), snapshot_loc).await?;

        let min_history_len = if self.truncate_all { 0 } else { 1 };

        // short cut
        if snapshots.len() <= min_history_len {
            return Ok(None);
        }

        let current_segments: HashSet<&String>;
        let current_snapshot: TableSnapshot;
        if self.truncate_all {
            // if truncate_all requested, gc root contains nothing;
            current_segments = HashSet::new();
        } else {
            current_snapshot = snapshots.remove(0);
            current_segments = HashSet::from_iter(&current_snapshot.segments);
        }

        let prevs = snapshots.iter().fold(HashSet::new(), |mut acc, s| {
            acc.extend(&s.segments);
            acc
        });

        // segments which no longer need to be kept
        let seg_delta = prevs.difference(&current_segments).collect::<Vec<_>>();

        // blocks to be removed
        let prev_blocks: HashSet<String> = self.blocks_of(da.clone(), seg_delta.iter()).await?;
        let current_blocks: HashSet<String> =
            self.blocks_of(da.clone(), current_segments.iter()).await?;
        let block_delta = prev_blocks.difference(&current_blocks);

        // NOTE: the following actions are NOT transactional yet

        // 1. remove blocks
        let mut block_removed = 0u64;
        for x in block_delta {
            self.remove_location(da.clone(), x).await?;
            block_removed += 1;
        }

        // 2. remove the segments
        let mut segment_removed = 0u64;
        for x in seg_delta {
            self.remove_location(da.clone(), x).await?;
            segment_removed += 1;
        }

        // 3. remove the snapshots
        for x in snapshots.iter().rev() {
            let loc = snapshot_location(&x.snapshot_id);
            self.remove_location(da.clone(), loc).await?
        }

        let snapshot_removed = snapshots.len() as u64;

        Ok(Some((snapshot_removed, segment_removed, block_removed)))
    }

    fn empty_result(&self) -> Result<SendableDataBlockStream> {
        self.build_result(vec![])
    }

    fn build_result(&self, blocks: Vec<DataBlock>) -> Result<SendableDataBlockStream> {
        Ok(Box::pin(DataBlockStream::create(
            self.table_info.schema(),
            None,
            blocks,
        )))
    }

    async fn blocks_of(
        &self,
        data_accessor: Arc<dyn DataAccessor>,
        locations: impl Iterator<Item = impl AsRef<str>>,
    ) -> Result<HashSet<String>> {
        let mut result = HashSet::new();
        for x in locations {
            let res: SegmentInfo = io::read_obj(data_accessor.as_ref(), x).await?;
            for block_meta in res.blocks {
                result.insert(block_meta.location.path);
            }
        }
        Ok(result)
    }

    async fn remove_location(
        &self,
        data_accessor: Arc<dyn DataAccessor>,
        location: impl AsRef<str>,
    ) -> Result<()> {
        data_accessor.remove(location.as_ref()).await
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
        let flag_truncate_all = if self.truncate_all { "all" } else { "" };
        Some(vec![
            string_literal(self.arg_database_name.as_str()),
            string_literal(self.arg_table_name.as_str()),
            string_literal(flag_truncate_all),
        ])
    }

    async fn read(
        &self,
        ctx: Arc<QueryContext>,
        _plan: &ReadDataSourcePlan,
    ) -> Result<SendableDataBlockStream> {
        if let Some((snapshot_removed, segment_removed, block_removed)) =
            self.truncate_history(ctx).await?
        {
            let block = DataBlock::create_by_array(self.table_info.schema(), vec![
                Series::new(vec![snapshot_removed]),
                Series::new(vec![segment_removed]),
                Series::new(vec![block_removed]),
            ]);

            self.build_result(vec![block])
        } else {
            self.empty_result()
        }
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
