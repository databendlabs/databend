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
use std::sync::LazyLock;

use databend_common_ast::ast::WithOptions;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

pub const WITH_OPT_KEY_CONSUME: &str = "consume";
pub const WITH_OPT_KEY_MAX_BATCH_SIZE: &str = "max_batch_size";

/// Table option keys that can occur in 'create table statement'.
pub static TABLE_WITH_OPTIONS: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    let mut r = HashSet::new();
    r.insert(WITH_OPT_KEY_CONSUME);
    r.insert(WITH_OPT_KEY_MAX_BATCH_SIZE);

    r
});

pub fn check_with_opt_valid(with_options: &WithOptions) -> Result<()> {
    for with_option in with_options.options.iter() {
        let key = with_option.0.to_lowercase();
        if !TABLE_WITH_OPTIONS.contains(key.to_lowercase().as_str()) {
            return Err(ErrorCode::WithOptionInvalid(format!(
                "with option {key} is invalid",
            )));
        }
    }
    Ok(())
}

pub fn get_with_opt_consume(with_options: &WithOptions) -> Result<bool> {
    with_options
        .options
        .get(WITH_OPT_KEY_CONSUME)
        .map(|value| value.parse::<bool>().map_err(|e| e.into()))
        .unwrap_or(Ok(false))
}

pub fn get_with_opt_max_batch_size(with_options: &WithOptions) -> Result<Option<u64>> {
    with_options
        .options
        .get(WITH_OPT_KEY_MAX_BATCH_SIZE)
        .map(|value| value.parse::<u64>().map_err(|err| err.into()))
        .transpose()
}
