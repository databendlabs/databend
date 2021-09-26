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

use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
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
use crate::catalogs::TableInfo;
use crate::datasources::dal::DataAccessor;
use crate::datasources::table::fuse::range_filter;
use crate::datasources::table::fuse::read_part;
use crate::datasources::table::fuse::read_table_snapshot;
use crate::datasources::table::fuse::segment_info_location;
use crate::datasources::table::fuse::snapshot_location;
use crate::datasources::table::fuse::BlockLocation;
use crate::datasources::table::fuse::MetaInfoReader;
use crate::datasources::table::fuse::SegmentInfo;
use crate::datasources::table::fuse::TableSnapshot;
use crate::datasources::table::fuse::TableStorageScheme;
use crate::sessions::DatabendQueryContextRef;

pub struct FuseTable {
    pub(crate) tbl_info: TableInfo,
    pub(crate) storage_scheme: TableStorageScheme,
}

impl FuseTable {
    pub fn try_create(_tbl_info: TableInfo) -> Result<Box<dyn Table>> {
        todo!()
    }

    //    pub fn with_meta_client(
    //        db: String,
    //        name: String,
    //        schema: DataSchemaRef,
    //        options: TableOptions,
    //        meta_client: T,
    //    ) -> Result<Box<dyn Table>> {
    //        let storage_scheme = parse_storage_scheme(options.get("STORAGE_SCHEME"))?;
    //        let res = FuseTable {
    //            db,
    //            name,
    //            schema,
    //            storage_scheme,
    //            meta_client,
    //            local: true,
    //        };
    //
    //        Ok(Box::new(res))
    //    }
}

#[async_trait::async_trait]
impl Table for FuseTable {
    fn name(&self) -> &str {
        &self.tbl_info.name
    }

    fn database(&self) -> &str {
        &self.tbl_info.db
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
        ctx: DatabendQueryContextRef,
        push_downs: Option<Extras>,
        _partition_num_hint: Option<usize>,
    ) -> Result<ReadDataSourcePlan> {
        // primary work to do: partition pruning/elimination
        let tbl_snapshot = self.table_snapshot(&ctx)?;
        if let Some(snapshot) = tbl_snapshot {
            let da = self.data_accessor(&ctx)?;
            let meta_reader = MetaInfoReader::new(da, ctx.clone());
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
                remote: true,
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
        ctx: DatabendQueryContextRef,
        source_plan: &ReadDataSourcePlan,
    ) -> Result<SendableDataBlockStream> {
        let default_proj = || {
            (0..self.tbl_info.schema.fields().len())
                .into_iter()
                .collect::<Vec<usize>>()
        };

        let projection = if let Some(push_down) = &source_plan.push_downs {
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
        let da = self.data_accessor(&ctx)?;
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
        ctx: DatabendQueryContextRef,
        insert_plan: InsertIntoPlan,
    ) -> Result<()> {
        // 1. take out input stream from plan
        //    Assumes that, insert_interpreter has already split data into blocks properly
        let block_stream = {
            match insert_plan.input_stream.lock().take() {
                Some(s) => s,
                None => return Err(ErrorCode::EmptyData("input stream consumed")),
            }
        };

        let data_accessor = self.data_accessor(&ctx)?;

        // 2. Append blocks to storage
        let segment_info = self.append_blocks(ctx.clone(), block_stream).await?;
        let seg_loc = {
            let uuid = Uuid::new_v4().to_simple().to_string();
            segment_info_location(&uuid)
        };
        self.save_segment(&seg_loc, &data_accessor, segment_info)
            .await?;

        // 3. new snapshot
        let tbl_snapshot = self
            .table_snapshot(&ctx)?
            .unwrap_or_else(TableSnapshot::new);
        let _snapshot_id = tbl_snapshot.snapshot_id;
        let new_snapshot: TableSnapshot = self.merge_seg(seg_loc, tbl_snapshot);
        let _new_snapshot_id = new_snapshot.snapshot_id;

        let snapshot_loc = {
            let uuid = Uuid::new_v4().to_simple().to_string();
            snapshot_location(&uuid)
        };

        self.save_snapshot(&snapshot_loc, &data_accessor, new_snapshot)
            .await?;

        // 4. commit
        let _table_id = insert_plan.tbl_id;
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
        _ctx: DatabendQueryContextRef,
        _truncate_plan: TruncateTablePlan,
    ) -> Result<()> {
        todo!()
    }
}

impl FuseTable {
    fn table_snapshot(&self, ctx: &DatabendQueryContextRef) -> Result<Option<TableSnapshot>> {
        let schema = self.schema()?;
        if let Some(loc) = schema.meta().get("META_SNAPSHOT_LOCATION") {
            let r = read_table_snapshot(self.data_accessor(ctx)?, ctx, loc)?;
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
            remote: true,
            tbl_args: None,
            push_downs: None,
        })
    }

    pub(crate) fn to_partitions(&self, _blocs: &[BlockLocation]) -> (Statistics, Partitions) {
        todo!()
    }

    pub(crate) fn data_accessor(
        &self,
        ctx: &DatabendQueryContextRef,
    ) -> Result<Arc<dyn DataAccessor>> {
        let scheme = &self.storage_scheme;
        ctx.get_data_accessor(scheme)
    }
}

impl FuseTable {
    pub(crate) async fn save_segment(
        &self,
        location: &str,
        data_accessor: &Arc<dyn DataAccessor>,
        segment_info: SegmentInfo,
    ) -> Result<()> {
        let bytes = serde_json::to_vec(&segment_info)?;
        data_accessor.put(location, bytes).await
    }
    pub(crate) async fn save_snapshot(
        &self,
        location: &str,
        data_accessor: &Arc<dyn DataAccessor>,
        snapshot: TableSnapshot,
    ) -> Result<()> {
        let bytes = serde_json::to_vec(&snapshot)?;
        data_accessor.put(location, bytes).await
    }
    pub(crate) fn merge_seg(&self, new_seg: String, mut prev: TableSnapshot) -> TableSnapshot {
        prev.segments.push(new_seg);
        let new_id = Uuid::new_v4();
        prev.snapshot_id = new_id;
        prev
    }
}
