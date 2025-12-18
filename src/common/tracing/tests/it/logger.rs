// Copyright 2023 Datafuse Labs.
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

use std::collections::BTreeMap;

use databend_common_tracing::Config;
use databend_common_tracing::init_logging;

/// Test rolling files logger
/// generate 30 log files, then check if there are 2 files
#[test]
fn test_rolling_files() {
    let mut config = Config::new_testing();
    // set limit to 2
    config.file.limit = 2;
    // set log file max_size to 16 bytes, in order to generate more log files
    config.file.max_size = 16;
    let _drop_guards = init_logging("test", &config, BTreeMap::default());

    for _ in 0..30 {
        log::error!("Hello error!");
        log::warn!("Hello warn!");
        log::info!("Hello info!");
        log::debug!("Hello debug!");
        log::trace!("Hello trace!");
        std::thread::sleep(std::time::Duration::from_millis(100));
    }

    let mut file_count = 0;
    for entry in std::fs::read_dir(config.file.dir.clone()).unwrap() {
        let entry = entry.unwrap();
        if entry.file_name().to_string_lossy().starts_with("test.") {
            file_count += 1;
        }
    }

    assert!(file_count <= config.file.limit);
}
