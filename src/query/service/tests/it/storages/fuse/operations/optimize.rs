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

use common_base::base::tokio;
use common_exception::Result;

use crate::storages::fuse::utils::do_purge_test;
use crate::storages::fuse::utils::TestTableOperation;

#[tokio::test]
async fn test_fuse_snapshot_optimize() -> Result<()> {
    do_purge_test(
        "implicit pure",
        TestTableOperation::Optimize("".to_string()),
        1,
        0,
        1,
        1,
        1,
        None,
    )
    .await
}

#[tokio::test]
async fn test_fuse_snapshot_optimize_purge() -> Result<()> {
    do_purge_test(
        "explicit pure",
        TestTableOperation::Optimize("purge".to_string()),
        1,
        0,
        1,
        1,
        1,
        None,
    )
    .await
}

#[tokio::test]
async fn test_fuse_snapshot_optimize_all() -> Result<()> {
    do_purge_test(
        "explicit pure",
        TestTableOperation::Optimize("all".to_string()),
        1,
        0,
        1,
        1,
        1,
        None,
    )
    .await
}
