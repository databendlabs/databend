// Copyright 2021 Datafuse Labs.
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

use clap::Parser;
use common_base::tokio;
use common_meta_raft_store::config::RaftConfig;
use common_meta_sled_store::get_sled_db;
use common_meta_sled_store::init_sled_db;
use common_tracing::init_global_tracing;
use databend_meta::export::exported_line_to_json;

/// Usage:
/// - To dump a sled db: `$0 --raft-dir ./_your_meta_dir/`:
///   ```
///   ["global-local-kvstate_machine/0",7,"sledks::Sequences","generic-kv",13]
///   ["global-local-kvstate_machine/0",7,"sledks::Sequences","table-lookup",1]
///   ["global-local-kvstate_machine/0",7,"sledks::Sequences","table_id",1]
///   ["global-local-kvstate_machine/0",7,"sledks::Sequences","tables",1]
///   ["global-local-kvstate_machine/0",8,"sledks::Databases",1,{"seq":1,"meta":null,"data":{"engine":"","engine_options":{},"options":{},"created_on":"2022-02-16T03:20:26.007286Z"}}]
///   ```
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let raft_config: RaftConfig = RaftConfig::parse();

    let _guards = init_global_tracing("metactl", "./_metactl_log", "debug");

    println!("raft_config: {:?}", raft_config);

    init_sled_db(raft_config.raft_dir.clone());

    print_meta().await
}

/// Print the entire sled db.
///
/// The output encodes every key-value into one line:
/// `[sled_tree_name, subtree_prefix(u8), subtree_prefix(str), key, value]`
/// E.g.:
/// `["global-local-kv/state_machine/0",7,"sledks::Sequences","databases",1]`
async fn print_meta() -> anyhow::Result<()> {
    let db = get_sled_db();

    let tree_names = db.tree_names();
    for n in tree_names.iter() {
        let name = String::from_utf8(n.to_vec())?;

        let tree = db.open_tree(&name)?;
        for x in tree.iter() {
            let kv = x?;
            let kv = vec![kv.0.to_vec(), kv.1.to_vec()];

            let line = exported_line_to_json(&name, &kv)?;
            println!("{}", line);
        }
    }

    Ok(())
}
