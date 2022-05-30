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

use std::collections::HashMap;
use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRefExt;
use common_exception::Result;
use common_planners::Expression;
use common_planners::Extras;
use common_planners::PartInfoPtr;
use common_planners::TruncateTablePlan;

use crate::pipelines::transforms::ExpressionExecutor;
use crate::sessions::QueryContext;
use crate::storages::fuse::fuse_part::ColumnMeta;
use crate::storages::fuse::fuse_part::FusePartInfo;
use crate::storages::fuse::meta::BlockMeta;
use crate::storages::fuse::operations::del::collector::Deletion;
use crate::storages::fuse::operations::del::collector::DeletionCollector;
use crate::storages::fuse::pruning::BlockPruner;
use crate::storages::fuse::FuseTable;
use crate::storages::Table;

impl FuseTable {
    pub async fn delete(&self, ctx: Arc<QueryContext>, push_downs: &Option<Extras>) -> Result<()> {
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
                    match self
                        .delete_from_block(&block_meta, &ctx, proj, filter)
                        .await?
                    {
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
                self.truncate(ctx.clone(), TruncateTablePlan {
                    catalog: "default".to_string(),
                    db: "".to_string(),
                    table: "".to_string(),
                    purge: false,
                })
                .await
            }
        } else {
            // no snapshot, no deletion
            Ok(())
        }
    }

    async fn delete_from_block(
        &self,
        block_meta: &BlockMeta,
        ctx: &Arc<QueryContext>,
        projection: Vec<usize>,
        expr: &Expression,
    ) -> Result<Deletion> {
        let part = projection_part(&block_meta, &projection);
        let reader = self.create_block_reader(ctx, projection)?;

        // read the cols that we are going to filter
        let data_block = reader.read(part).await?; // TODO refine this

        let schema = self.table_info.schema();
        let expr_field = expr.to_data_field(&schema)?;
        let expr_schema = DataSchemaRefExt::create(vec![expr_field]);

        // TODO inverse the expr
        let exec = ExpressionExecutor::try_create(
            ctx.clone(),
            "filter expression executor (delete) ",
            schema.clone(),
            expr_schema,
            vec![expr.clone()],
            false,
        )?;

        // TODO optimize this, check if filter_block are all zeros / ones before we return the block

        let filter_block = exec.execute(&data_block)?;
        // duplicated code
        let whole_table_proj = (0..self.table_info.schema().fields().len())
            .into_iter()
            .collect::<Vec<usize>>();
        let part_of_whole_table = projection_part(&block_meta, &whole_table_proj);

        // read the cols that we are going to filter
        // TODO enhance the reader, read without the PartInfor
        let whole_block = reader.read(part_of_whole_table).await?;

        // returns the data remains after deletion
        let data_block = DataBlock::filter_block(&whole_block, filter_block.column(0))?;
        let res = if data_block.num_rows() == block_meta.row_count as usize {
            // TODO: this works , but not acceptableA (the whole data block is cloned here)
            Deletion::NothingDeleted
        } else {
            Deletion::Remains(data_block)
        };
        Ok(res)
    }
    fn commit(&self, del_holder: DeletionCollector) -> Result<()> {
        //  let new_snapshot = self.update_snapshot(del_holder.updated_segments());
        // // try commit and detect conflicts
        todo!()
    }
}

// TODO duplicated code
fn projection_part(meta: &BlockMeta, projections: &[usize]) -> PartInfoPtr {
    let mut columns_meta = HashMap::with_capacity(projections.len());

    for projection in projections {
        let column_meta = &meta.col_metas[&(*projection as u32)];

        columns_meta.insert(
            *projection,
            ColumnMeta::create(column_meta.offset, column_meta.len, column_meta.num_values),
        );
    }

    let rows_count = meta.row_count;
    let location = meta.location.0.clone();
    let format_version = meta.location.1;
    // TODO
    // row_count should be a hint value of  LIMIT,
    // not the count the rows in this partition
    FusePartInfo::create(
        location,
        format_version,
        rows_count,
        columns_meta,
        meta.compression,
    )
}
