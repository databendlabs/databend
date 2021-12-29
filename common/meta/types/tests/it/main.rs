//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

mod cluster;
mod match_seq;
mod user_defined_function;
mod user_grant;
mod user_info;
mod user_privilege;
mod user_quota;
mod user_stage;

#[test]
fn test_bin_commit_version() -> anyhow::Result<()> {
    let v = &common_meta_types::config::DATABEND_COMMIT_VERSION;
    assert!(v.len() > 0);
    Ok(())
}
