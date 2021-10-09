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

use common_catalog::BlockLocation;
use common_catalog::IOContext;
use common_catalog::TableIOContext;
use common_catalog::TableSnapshot;
use common_dal::ObjectAccessor;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_flight::meta_flight_reply::TableInfo;
use common_planners::Extras;
use common_planners::InsertIntoPlan;
use common_planners::Partitions;
use common_planners::ReadDataSourcePlan;
use common_planners::Statistics;
use common_planners::TruncateTablePlan;
use common_streams::ProgressStream;
use common_streams::SendableDataBlockStream;
use tokio_stream::wrappers::ReceiverStream;
use uuid::Uuid;

use crate::catalogs::Table;
use crate::datasources::table::fuse::range_filter;
use crate::datasources::table::fuse::read_part;
use crate::datasources::table::fuse::segment_info_location;
use crate::datasources::table::fuse::snapshot_location;
use crate::datasources::table::fuse::BlockAppender;
use crate::datasources::table::fuse::MetaInfoReader;
use crate::sessions::DatabendQueryContext;

pub struct FuseTable {
    pub(crate) tbl_info: TableInfo,
}

impl FuseTable {
    pub fn try_create(tbl_info: TableInfo) -> Result<Box<dyn Table>> {
        Ok(Box::new(FuseTable { tbl_info }))
    }
}

#[async_trait::async_trait]
impl Table for FuseTable {
    fn name(&self) -> &str {
        &self.tbl_info.name
    }

    fn engine(&self) -> &str {
        &self.tbl_info.engine
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Result<DataSchemaRef> {
        Ok(self.tbl_info.schema.clone())
    }

    fn get_id(&self) -> u64 {
        self.tbl_info.table_id
    }

    fn is_local(&self) -> bool {
        false
    }

    fn read_plan(
        &self,
        io_ctx: Arc<TableIOContext>,
        push_downs: Option<Extras>,
        _partition_num_hint: Option<usize>,
    ) -> Result<ReadDataSourcePlan> {
        // primary work to do: partition pruning/elimination
        let tbl_snapshot = self.table_snapshot(io_ctx.clone())?;
        if let Some(snapshot) = tbl_snapshot {
            let da = io_ctx.get_data_accessor()?;

            let meta_reader = MetaInfoReader::new(da, io_ctx.get_runtime());
            let block_locations = range_filter(&snapshot, &push_downs, meta_reader)?;
            let (statistics, parts) = self.to_partitions(&block_locations);

            let plan = ReadDataSourcePlan {
                db: self.tbl_info.db.to_string(),
                table: self.name().to_string(),
                table_id: self.tbl_info.table_id,
                table_version: None,
                schema: self.tbl_info.schema.clone(),
                parts,
                statistics,
                description: "".to_string(),
                scan_plan: Default::default(),
                tbl_args: None,
                push_downs,
            };
            Ok(plan)
        } else {
            self.empty_read_source_plan()
        }
    }

    async fn read(
        &self,
        io_ctx: Arc<TableIOContext>,
        push_downs: &Option<Extras>,
    ) -> Result<SendableDataBlockStream> {
        let ctx: Arc<DatabendQueryContext> = io_ctx
            .get_user_data()?
            .expect("DatabendQueryContext should not be None");

        let default_proj = || {
            (0..self.tbl_info.schema.fields().len())
                .into_iter()
                .collect::<Vec<usize>>()
        };

        let projection = if let Some(push_down) = push_downs {
            if let Some(prj) = &push_down.projection {
                prj.clone()
            } else {
                default_proj()
            }
        } else {
            default_proj()
            //            todo!()
            // leave this to another PR
            //project_col_idx(
            //    &self.tbl_info.table_schema,
            //    &source_plan.push_downs.projected_schema,
            //)?;
        };

        let (tx, rx) = common_base::tokio::sync::mpsc::channel(1024);

        let bite_size = 1; // TODO config
        let mut iter = {
            let ctx = ctx.clone();
            std::iter::from_fn(move || match ctx.clone().try_get_partitions(bite_size) {
                Err(_) => None,
                Ok(parts) if parts.is_empty() => None,
                Ok(parts) => Some(parts),
            })
            .flatten()
        };
        let da = io_ctx.get_data_accessor()?;
        let arrow_schema = self.tbl_info.schema.to_arrow();
        let _h = common_base::tokio::task::spawn_local(async move {
            // TODO error handling is buggy
            for part in &mut iter {
                read_part(
                    part,
                    da.clone(),
                    projection.clone(),
                    tx.clone(),
                    &arrow_schema,
                )
                .await?;
            }
            Ok::<(), ErrorCode>(())
        });

        let progress_callback = ctx.progress_callback()?;
        let receiver = ReceiverStream::new(rx);
        let stream = ProgressStream::try_create(Box::pin(receiver), progress_callback)?;
        Ok(Box::pin(stream))
    }

    async fn append_data(
        &self,
        io_ctx: Arc<TableIOContext>,
        _insert_plan: InsertIntoPlan,
    ) -> Result<()> {
        // 1. take out input stream from plan
        //    Assumes that, insert_interpreter has already split data into blocks properly
        let block_stream = {
            match _insert_plan.input_stream.lock().take() {
                Some(s) => s,
                None => return Err(ErrorCode::EmptyData("input stream consumed")),
            }
        };

        let da = io_ctx.get_data_accessor()?;

        // 2. Append blocks to storage
        let segment_info = BlockAppender::append_blocks(da.clone(), block_stream).await?;

        let seg_loc = {
            let uuid = Uuid::new_v4().to_simple().to_string();
            segment_info_location(&uuid)
        };

        {
            let bytes = serde_json::to_vec(&segment_info)?;
            da.put(&seg_loc, bytes).await?;
        }

        // 3. new snapshot
        let tbl_snapshot = self
            .table_snapshot(io_ctx)?
            .unwrap_or_else(TableSnapshot::new);
        let _snapshot_id = tbl_snapshot.snapshot_id;
        let new_snapshot = tbl_snapshot.append_segment(seg_loc);
        let _new_snapshot_id = new_snapshot.snapshot_id;

        {
            let uuid = Uuid::new_v4().to_simple().to_string();
            let snapshot_loc = snapshot_location(&uuid);

            let bytes = serde_json::to_vec(&new_snapshot)?;
            da.put(&snapshot_loc, bytes).await?;
        }

        // 4. commit
        let _table_id = _insert_plan.tbl_id;
        // TODO simple retry strategy
        // self.meta_client
        //     .commit_table(
        //         table_id,
        //         snapshot_id.to_simple().to_string(),
        //         new_snapshot_id.to_simple().to_string(),
        //     )
        //     .await?;
        Ok(())
    }

    async fn truncate(
        &self,
        _io_ctx: Arc<TableIOContext>,
        _truncate_plan: TruncateTablePlan,
    ) -> Result<()> {
        todo!()
    }
}

impl FuseTable {
    fn table_snapshot(&self, io_ctx: Arc<TableIOContext>) -> Result<Option<TableSnapshot>> {
        let schema = &self.tbl_info.schema;
        if let Some(loc) = schema.meta().get("META_SNAPSHOT_LOCATION") {
            let da = io_ctx.get_data_accessor()?;
            let r = ObjectAccessor::new(da).blocking_read_obj(&io_ctx.get_runtime(), loc)?;
            Ok(Some(r))
        } else {
            Ok(None)
        }
    }

    pub(crate) fn empty_read_source_plan(&self) -> Result<ReadDataSourcePlan> {
        Ok(ReadDataSourcePlan {
            db: self.tbl_info.name.clone(),
            table: self.name().to_string(),
            table_id: self.tbl_info.table_id,
            table_version: None,
            schema: self.tbl_info.schema.clone(),
            parts: vec![],
            statistics: Statistics::default(),
            description: "".to_string(),
            scan_plan: Default::default(),
            tbl_args: None,
            push_downs: None,
        })
    }

    pub(crate) fn to_partitions(&self, _blocs: &[BlockLocation]) -> (Statistics, Partitions) {
        todo!()
    }
}
