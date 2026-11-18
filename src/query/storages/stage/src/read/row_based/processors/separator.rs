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
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use databend_common_base::base::ProgressValues;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_pipeline_transforms::processors::AccumulatingTransform;

use crate::read::load_context::LoadContext;
use crate::read::row_based::batch::BytesBatch;
use crate::read::row_based::format::RowBasedFileFormat;
use crate::read::row_based::format::SeparatorState;
use crate::read::row_based::processors::ordered::FileBatch;

pub struct Separator {
    pub ctx: Arc<LoadContext>,
    pub state: Option<Box<dyn SeparatorState>>,
    pub format: Arc<dyn RowBasedFileFormat>,
    /// For an order-preserving read: the source of file ids, shared by all separators. Each
    /// batch is then wrapped in a [`FileBatch`] with the id of the file it was read from, and
    /// the last batch of a file is followed by a [`FileBatch`] without one.
    pub file_ids: Option<Arc<AtomicU64>>,
    file_id: u64,
}

impl Separator {
    // TODO remove Result from return type
    pub fn try_create(ctx: Arc<LoadContext>, format: Arc<dyn RowBasedFileFormat>) -> Result<Self> {
        Ok(Separator {
            ctx,
            format,
            state: None,
            file_ids: None,
            file_id: 0,
        })
    }

    pub fn with_file_ids(mut self, file_ids: Arc<AtomicU64>) -> Self {
        self.file_ids = Some(file_ids);
        self
    }
}

impl AccumulatingTransform for Separator {
    const NAME: &'static str = "Separator";

    fn transform(&mut self, data: DataBlock) -> Result<Vec<DataBlock>> {
        let batch = data
            .get_owned_meta()
            .and_then(BytesBatch::downcast_from)
            .unwrap();

        if self.state.is_none()
            && let Some(file_ids) = &self.file_ids
        {
            // Every read of a file gets its own id, even when FILES lists the file twice.
            self.file_id = file_ids.fetch_add(1, Ordering::Relaxed);
        }
        let state = self.state.get_or_insert_with(|| {
            self.format
                .try_create_separator(self.ctx.clone(), &batch.path)
                .unwrap()
        });
        let mut process_values = ProgressValues { rows: 0, bytes: 0 };

        let batch_meta = batch.meta();
        let (row_batches, file_status) = state.append(batch)?;
        let row_batches = row_batches
            .into_iter()
            .filter(|b| b.data.rows() > 0 || b.data.size() > 0)
            .collect::<Vec<_>>();
        // Errors tolerated by the separator (e.g. column count mismatch) are only reported
        // in the status of the batch that contains them; `add_chunk` merges per file.
        if file_status.error.is_some() {
            self.ctx
                .table_context
                .copy_state()
                .copy_status()
                .add_chunk(&batch_meta.path, file_status);
        }
        if batch_meta.is_eof {
            self.state = None;
        }
        let ordered = self.file_ids.is_some();
        let file_id = self.file_id;
        let file_end = (batch_meta.is_eof && ordered).then(|| {
            DataBlock::empty_with_meta(Box::new(FileBatch {
                file_id,
                path: batch_meta.path.clone(),
                batch: None,
            }))
        });
        if row_batches.is_empty() {
            Ok(file_end.into_iter().collect())
        } else {
            for b in row_batches.iter() {
                process_values.rows += b.data.rows();
            }
            self.ctx
                .table_context
                .get_scan_progress()
                .incr(&process_values);

            let blocks = row_batches
                .into_iter()
                .map(|b| {
                    if ordered {
                        DataBlock::empty_with_meta(Box::new(FileBatch {
                            file_id,
                            path: b.start_pos.path.clone(),
                            batch: Some(b),
                        }))
                    } else {
                        DataBlock::empty_with_meta(Box::new(b))
                    }
                })
                .chain(file_end)
                .collect::<Vec<_>>();
            Ok(blocks)
        }
    }
}
