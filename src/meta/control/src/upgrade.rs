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

use databend_meta_raft_store::config::RaftConfig;
use databend_meta_raft_store::ondisk::OnDisk;
use databend_meta_runtime_api::SpawnApi;

/// Upgrade the data in raft_dir to the latest version.
pub async fn upgrade<SP: SpawnApi>(raft_config: &RaftConfig) -> anyhow::Result<()> {
    let mut on_disk = OnDisk::open(raft_config).await?;
    on_disk.log_stderr(true);
    on_disk.upgrade::<SP>().await?;

    Ok(())
}
