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
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_catalog::plan::TopK;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchema;
use databend_common_expression::Scalar;
use databend_common_expression::TopKSorter;
use databend_common_expression::types::DataType;
use databend_common_expression::types::MutableBitmap;
use parking_lot::Mutex;
use tokio::sync::Notify;

const MAX_PROGRESSIVE_TOPK_LIMIT: usize = 1000;

pub struct ProgressiveTopKState {
    sorter: Mutex<TopKSorter>,
    sort_column_offset: usize,
    scheduled_parts: AtomicUsize,
    completed_parts: AtomicUsize,
    skipped_blocks: AtomicUsize,
    notify: Notify,
}

impl ProgressiveTopKState {
    pub(crate) fn try_create(
        top_k: Option<&TopK>,
        output_schema: &DataSchema,
    ) -> Option<Arc<Self>> {
        let top_k = top_k?;
        if top_k.limit > MAX_PROGRESSIVE_TOPK_LIMIT {
            return None;
        }

        let data_type = DataType::from(top_k.field.data_type());
        if !supports_progressive_topk(&data_type) {
            return None;
        }

        let sort_column_offset = output_schema.index_of(top_k.field.name()).ok()?;
        Some(Arc::new(Self {
            sorter: Mutex::new(TopKSorter::new(top_k.limit, top_k.asc)),
            sort_column_offset,
            scheduled_parts: AtomicUsize::new(0),
            completed_parts: AtomicUsize::new(0),
            skipped_blocks: AtomicUsize::new(0),
            notify: Notify::new(),
        }))
    }

    pub(crate) async fn wait_for_previous_part(&self) {
        loop {
            let scheduled = self.scheduled_parts.load(Ordering::Acquire);
            let completed = self.completed_parts.load(Ordering::Acquire);
            if scheduled == completed {
                return;
            }
            self.notify.notified().await;
        }
    }

    pub(crate) fn record_scheduled_part(&self) {
        self.scheduled_parts.fetch_add(1, Ordering::AcqRel);
    }

    pub(crate) fn complete_part(&self, data_block: &DataBlock) {
        self.update(data_block);
        self.completed_parts.fetch_add(1, Ordering::AcqRel);
        self.notify.notify_waiters();
    }

    fn update(&self, data_block: &DataBlock) {
        let rows = data_block.num_rows();
        if rows == 0 {
            return;
        }

        let column = data_block
            .get_by_offset(self.sort_column_offset)
            .to_column();
        let mut bitmap = MutableBitmap::from_len_set(rows);
        self.sorter.lock().push_column(&column, &mut bitmap);
    }

    pub(crate) fn never_match(&self, sort_min_max: &(Scalar, Scalar)) -> bool {
        self.sorter.lock().never_match(sort_min_max)
    }

    pub(crate) fn record_skipped_blocks(&self, skipped_blocks: usize) {
        if skipped_blocks == 0 {
            return;
        }

        self.skipped_blocks
            .fetch_add(skipped_blocks, Ordering::Relaxed);
        Profile::record_usize_profile(
            ProfileStatisticsName::ProgressiveTopKPruneParts,
            skipped_blocks,
        );
    }
}

fn supports_progressive_topk(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Number(_) | DataType::Date | DataType::Timestamp
    )
}
