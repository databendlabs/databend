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

//! Integration test for SpillTarget::from_storage_params semantics.

use databend_common_meta_app::storage::StorageFsConfig;
use databend_common_meta_app::storage::StorageParams;
use databend_query::spillers::SpillTarget;

#[test]
fn test_spill_target_from_storage_params() {
    // Fs backend should be treated as local spill.
    let fs_params = StorageParams::Fs(StorageFsConfig {
        root: "/tmp/test-root".to_string(),
    });
    let target = SpillTarget::from_storage_params(Some(&fs_params));
    assert!(target.is_local());

    // None (or any non-Fs backend) should be treated as remote spill.
    let target_none = SpillTarget::from_storage_params(None);
    assert!(!target_none.is_local());
}
