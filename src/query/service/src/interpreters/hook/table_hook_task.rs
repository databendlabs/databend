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
use std::time::Instant;

use databend_common_catalog::lock::LockTableOption;
use databend_common_pipeline::core::SharedLockGuard;

use crate::interpreters::hook::TableHookTarget;
use crate::sessions::QueryContext;

/// Foreground hook state, never retained by the background queue.
pub(super) struct TableHookTask<'a> {
    pub(super) ctx: &'a Arc<QueryContext>,
    pub(super) target: &'a TableHookTarget,
    pub(super) compact_after_write: bool,
    pub(super) lock_opt: LockTableOption,
    pub(super) lock_guard: Option<SharedLockGuard>,
    pub(super) operation_name: String,
    pub(super) main_operation_start: Instant,
}
