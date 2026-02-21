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

use databend_meta_raft_store::key_spaces::RaftStoreEntry;
use databend_meta_sled_store::init_get_sled_db;

/// Rewrite protobuf encoded logs and applied record in a sled db.
///
/// The `convert` rewrite an entry if needed.
/// If nothing needs to do, it should return `Ok(None)`
pub fn process_sled_db<F>(raft_dir: impl Into<String>, convert: F) -> anyhow::Result<()>
where F: Fn(RaftStoreEntry) -> Result<Option<RaftStoreEntry>, anyhow::Error> {
    let db = init_get_sled_db(raft_dir.into(), 1024 * 1024 * 1024);

    let mut tree_names = db.tree_names();
    tree_names.sort();

    for n in tree_names.iter() {
        let name = String::from_utf8(n.to_vec())?;
        let tree = db.open_tree(&name)?;

        let mut converted = vec![];
        for x in tree.iter() {
            let kv = x?;
            let k = kv.0.to_vec();
            let v = kv.1.to_vec();

            let v1_ent = RaftStoreEntry::deserialize(&k, &v)?;
            let res: Option<RaftStoreEntry> = convert(v1_ent.clone())?;

            if let Some(v2_ent) = res {
                converted.push((v1_ent, v2_ent));
            }
        }

        // write back
        eprintln!(
            "Start to write back converted record, tree: {}, records count: {}",
            name,
            converted.len()
        );

        for (_v1_ent, v2_ent) in converted {
            let (k, v) = RaftStoreEntry::serialize(&v2_ent)?;
            tree.insert(k, v)?;
        }

        tree.flush()?;

        eprintln!("Done writing back converted record, tree: {}", name,);
    }

    eprintln!("All converted");

    Ok(())
}
