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

use std::time::Duration;

use chrono::DateTime;
use chrono::Utc;

const DEFAULT_DATA_RETENTION_SECONDS: i64 = 24 * 60 * 60;

/// Determines if an item is within the retention period based on its drop time.
///
/// # Arguments
/// * `drop_on` - The optional timestamp when the item was marked for deletion.
/// * `now` - The current timestamp used as a reference point.
///
/// Items without a drop time (`None`) are always considered retainable.
/// The retention period is defined by `DEFAULT_DATA_RETENTION_SECONDS`.
pub fn is_drop_time_retainable(drop_on: Option<DateTime<Utc>>, now: DateTime<Utc>) -> bool {
    let retention_boundary = get_retention_boundary(now);

    // If it is None, fill it with a very big time.
    let drop_on = drop_on.unwrap_or(DateTime::<Utc>::MAX_UTC);
    drop_on > retention_boundary
}

/// Get the retention boundary time before which the data can be permanently removed.
pub fn get_retention_boundary(now: DateTime<Utc>) -> DateTime<Utc> {
    now - Duration::from_secs(DEFAULT_DATA_RETENTION_SECONDS as u64)
}
