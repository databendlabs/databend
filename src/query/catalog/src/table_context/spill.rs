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

use databend_common_base::base::Progress;
use databend_common_base::base::ProgressValues;

pub trait TableContextSpillProgress: Send + Sync {
    fn get_join_spill_progress(&self) -> Arc<Progress>;

    fn get_group_by_spill_progress(&self) -> Arc<Progress>;

    fn get_aggregate_spill_progress(&self) -> Arc<Progress>;

    fn get_window_partition_spill_progress(&self) -> Arc<Progress>;

    fn get_join_spill_progress_value(&self) -> ProgressValues;

    fn get_group_by_spill_progress_value(&self) -> ProgressValues;

    fn get_aggregate_spill_progress_value(&self) -> ProgressValues;

    fn get_window_partition_spill_progress_value(&self) -> ProgressValues;
}
