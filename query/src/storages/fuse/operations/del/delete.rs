//  Copyright 2022 Datafuse Labs.
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

use common_exception::Result;
use common_planners::DeletePlan;
use common_planners::Extras;

use super::filter::delete_from_block;
use crate::sessions::QueryContext;
use crate::storages::fuse::operations::del::collector::Deletion;
use crate::storages::fuse::operations::del::collector::DeletionCollector;
use crate::storages::fuse::pruning::BlockPruner;
use crate::storages::fuse::FuseTable;

impl FuseTable {
    pub async fn delete(
        &self,
        ctx: Arc<QueryContext>,
        push_downs: &Option<Extras>,
        plan: &DeletePlan,
    ) -> Result<()> {
        let snapshot = self.read_table_snapshot(ctx.as_ref()).await?;
        if let Some(snapshot) = snapshot {
            if let Some(Extras { filters, .. }) = &push_downs {
                let filter = &filters[0]; // TODO err handling, filters.len SHOULD equal one
                let mut deleter = DeletionCollector::new(self, ctx.as_ref());
                let schema = self.table_info.schema();
                let block_metas = BlockPruner::new(snapshot.clone())
                    .apply(ctx.as_ref(), schema, &push_downs)
                    .await?;

                // delete block one by one.
                // this could be executed in a distributed manner, till new planner, pipeline settled down totally
                for (seg_idx, block_meta) in block_metas {
                    let proj = self.projection_of_push_downs(push_downs);
                    match delete_from_block(self, &block_meta, &ctx, proj, filter).await? {
                        Deletion::NothingDeleted => {
                            // false positive, we should keep the whole block
                            continue;
                        }
                        Deletion::Remains(r) => {
                            // after deletion, the data block `r` remains, let keep it  by replacing the block
                            // located at `block_meta.location`, of segment indexed by `seg_idx`, with a new block `r`
                            deleter
                                .replace_with(seg_idx, &block_meta.location, r)
                                .await?
                        }
                    }
                }
                self.commit(deleter)
            } else {
                // deleting the whole table... just a truncate
                let purge = false;
                self.do_truncate(ctx.clone(), purge, plan.catalog_name.as_str())
                    .await
            }
        } else {
            // no snapshot, no deletion
            Ok(())
        }
    }

    fn commit(&self, _del_holder: DeletionCollector) -> Result<()> {
        //  let new_snapshot = self.update_snapshot(del_holder.updated_segments());
        // // try commit and detect conflicts
        todo!()
    }
}
