// Copyright 2020 Datafuse Labs.
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

use common_exception::Result;

#[test]
fn test_progress() -> Result<()> {
    use crate::*;

    let progress = Progress::create();
    let values = ProgressValues {
        read_rows: 2,
        read_bytes: 10,
        total_rows_to_read: 10,
    };

    progress.incr(&values);

    assert_eq!(2, progress.get_values().read_rows);
    assert_eq!(10, progress.get_values().read_bytes);
    progress.reset();

    assert_eq!(0, progress.get_values().read_rows);
    assert_eq!(0, progress.get_values().read_bytes);
    Ok(())
}
