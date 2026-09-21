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

use std::collections::HashSet;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::Schema as ArrowSchema;
use async_trait::async_trait;
use databend_common_base::base::Progress;
use databend_common_base::base::ProgressValues;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_pipeline_transforms::processors::AsyncAccumulatingTransform;
use paimon::table::TableWrite;

use crate::error::map_paimon_error;
use crate::write::meta::PaimonCommitMeta;
use crate::write::router::PaimonWriteRouter;
use crate::write::router::next_writer_lane_id;

/// Transform that writes DataBlocks into a Paimon `TableWrite` and emits
/// serializable commit meta on finish.
pub struct PaimonTableWriter {
    write_progress: Arc<Progress>,
    writer: TableWrite,
    arrow_schema: Arc<ArrowSchema>,
    observe: Option<LaneObserveCtx>,
}

struct LaneObserveCtx {
    executor_id: String,
    lane_id: u64,
    router: PaimonWriteRouter,
    routes: HashSet<Vec<u8>>,
}

impl PaimonTableWriter {
    pub fn try_create(ctx: Arc<dyn TableContext>, table: paimon::Table) -> Result<Self> {
        let observe = if is_fixed_bucket_primary_key_table(&table) {
            Some(LaneObserveCtx {
                executor_id: ctx.get_cluster().local_id.clone(),
                lane_id: next_writer_lane_id(),
                router: PaimonWriteRouter::try_create(table.schema())?,
                routes: HashSet::new(),
            })
        } else {
            None
        };
        let mut writer = Self::try_create_with_progress(ctx.get_write_progress(), table)?;
        writer.observe = observe;
        Ok(writer)
    }

    /// Test/helper constructor that avoids requiring a full `TableContext`.
    pub fn try_create_with_progress(
        write_progress: Arc<Progress>,
        table: paimon::Table,
    ) -> Result<Self> {
        let arrow_schema = paimon::arrow::build_target_arrow_schema(table.schema().fields())
            .map_err(map_paimon_error)?;
        let writer = table
            .new_write_builder()
            .new_write()
            .map_err(map_paimon_error)?;
        Ok(Self {
            write_progress,
            writer,
            arrow_schema,
            observe: None,
        })
    }

    fn observe_routes(&mut self, batch: &RecordBatch) -> Result<()> {
        let Some(observe) = self.observe.as_mut() else {
            return Ok(());
        };
        let routes = observe.router.route_batch(batch)?;
        for route in routes {
            observe.routes.insert(route.key);
        }
        Ok(())
    }
}

fn is_fixed_bucket_primary_key_table(table: &paimon::Table) -> bool {
    let schema = table.schema();
    if schema.primary_keys().is_empty() {
        return false;
    }
    paimon::spec::CoreOptions::new(schema.options()).bucket() >= 1
}

#[async_trait]
impl AsyncAccumulatingTransform for PaimonTableWriter {
    const NAME: &'static str = "PaimonTableWriter";

    async fn transform(&mut self, block: DataBlock) -> Result<Option<DataBlock>> {
        if block.is_empty() {
            return Ok(None);
        }
        let rows = block.num_rows();
        let batch = data_block_to_paimon_batch(block, &self.arrow_schema)?;
        self.observe_routes(&batch)?;
        self.writer
            .write_arrow_batch(&batch)
            .await
            .map_err(map_paimon_error)?;
        self.write_progress.incr(&ProgressValues {
            rows,
            bytes: batch.get_array_memory_size(),
        });
        Ok(None)
    }

    async fn on_finish(&mut self, _output: bool) -> Result<Option<DataBlock>> {
        let messages = self
            .writer
            .prepare_commit()
            .await
            .map_err(map_paimon_error)?;
        if messages.is_empty() {
            return Ok(None);
        }
        let mut meta = PaimonCommitMeta::try_from_messages(messages)?;
        if let Some(observe) = self.observe.as_ref() {
            meta.route_owners = observe
                .routes
                .iter()
                .cloned()
                .map(|key| (key, observe.executor_id.clone(), observe.lane_id))
                .collect();
        }
        Ok(Some(DataBlock::empty_with_meta(Box::new(meta))))
    }
}

/// Convert a Databend `DataBlock` into a Paimon-target Arrow `RecordBatch`.
///
/// Reuses the same `build_target_arrow_schema` mapping as the read path
/// (`paimon_schema_to_databend`), then casts column arrays to that schema
/// (e.g. Databend `Utf8View` → Paimon `Utf8`).
pub fn data_block_to_paimon_batch(
    block: DataBlock,
    arrow_schema: &Arc<ArrowSchema>,
) -> Result<RecordBatch> {
    let num_rows = block.num_rows();
    if arrow_schema.fields().is_empty() {
        return Ok(RecordBatch::try_new_with_options(
            arrow_schema.clone(),
            vec![],
            &arrow_array::RecordBatchOptions::default().with_row_count(Some(num_rows)),
        )?);
    }

    if block.columns().len() != arrow_schema.fields().len() {
        return Err(ErrorCode::Internal(format!(
            "Paimon write column count mismatch: block={}, schema={}",
            block.columns().len(),
            arrow_schema.fields().len()
        )));
    }

    let mut arrays = Vec::with_capacity(block.columns().len());
    for (entry, field) in block.columns().iter().zip(arrow_schema.fields()) {
        let array = entry.to_column().maybe_gc().into_arrow_rs();
        let array = if array.data_type() == field.data_type() {
            array
        } else {
            arrow_cast::cast(array.as_ref(), field.data_type()).map_err(|err| {
                ErrorCode::Internal(format!(
                    "Failed to cast column '{}' to {:?}: {err}",
                    field.name(),
                    field.data_type()
                ))
            })?
        };
        arrays.push(array);
    }

    Ok(RecordBatch::try_new(arrow_schema.clone(), arrays)?)
}
