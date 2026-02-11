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

#![allow(clippy::uninlined_format_args)]

//! Process metadata for upgrading purpose.
//!
//! [`examples::upgrade_09`] loads metadata from a `raft-dir` and upgrade TableMeta to new version and write them back.
//! Both in raft-log data and in state-machine data will be converted.
//!
//! Usage:
//!
//! - Shut down all databend-meta processes.
//!
//! - Backup before proceeding: https://docs.databend.com/guides/deploy/upgrade/backup-and-restore-schema
//!
//! - To view the current TableMeta version, print all TableMeta records with the following command:
//!   It should display a list of TableMeta record.
//!   You need to upgrade only if there is a `ver` that is lower than 24.
//!
//!    ```text
//!    $0 --cmd print --raft-dir "<./your/raft-dir/>"
//!    # output:
//!    # TableMeta { ver: 23, ..
//!    ```
//!
//! - Run it:
//!
//!   ```text
//!   $0 --cmd upgrade --raft-dir "<./your/raft-dir/>"
//!   ```
//!
//! - To assert upgrade has finished successfully, print all TableMeta records that are found in meta dir with the following command:
//!   It should display a list of TableMeta record with a `ver` that is greater or equal 24.
//!
//!    ```text
//!    $0 --cmd print --raft-dir "<./your/raft-dir/>"
//!    # output:
//!    # TableMeta { ver: 25, ..
//!    # TableMeta { ver: 25, ..
//!    ```

pub mod examples;
pub mod kv_processor;
pub mod pb_value_decoder;
pub mod process;
pub mod process_meta_dir;
pub mod rewrite_kv;
