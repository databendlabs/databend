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

use common_base::base::tokio;
use common_meta_sled_store::SledTree;
use common_meta_sled_store::Store;
use common_meta_types::Endpoint;
use common_meta_types::Node;

use crate::init_sled_ut;
use crate::testing::fake_key_spaces::Nodes;
use crate::testing::new_sled_test_context;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_sled_txn_tree_key_space_insert_get_remove() -> anyhow::Result<()> {
    // Test transactional API insert, get, remove on a sub key space of TransactionSledTree

    let (_log_guards, ut_span) = init_sled_ut!();
    let _ent = ut_span.enter();

    let tc = new_sled_test_context();
    let db = &tc.db;
    let tree = SledTree::open(db, tc.tree_name, true)?;

    let k = 100;

    tree.txn(false, |txn_tree| {
        // sub tree key space
        let nodes_ks = txn_tree.key_space::<Nodes>();

        let got = nodes_ks.insert(&101, &Node::new("foo", Endpoint::new("", 100)))?;

        assert!(got.is_none());

        let got = nodes_ks.insert(&k, &Node::new("n", Endpoint::new("", 100)))?;

        assert!(got.is_none());

        let got = nodes_ks.get(&k)?;

        assert_eq!(Some(Node::new("n", Endpoint::new("", 100))), got);

        let got = nodes_ks.insert(&k, &Node::new("m", Endpoint::new("", 101)))?;

        assert_eq!(Some(Node::new("n", Endpoint::new("", 100))), got);

        Ok(())
    })?;

    let got = tree.key_space::<Nodes>().get(&k)?;
    assert_eq!(Some(Node::new("m", Endpoint::new("", 101))), got);

    let got = tree.key_space::<Nodes>().get(&101)?;
    assert_eq!(Some(Node::new("foo", Endpoint::new("", 100))), got);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_sled_txn_tree_key_space_remove() -> anyhow::Result<()> {
    // Test transactional API insert, get, remove on a sub key space of TransactionSledTree

    let (_log_guards, ut_span) = init_sled_ut!();
    let _ent = ut_span.enter();

    let tc = new_sled_test_context();
    let db = &tc.db;
    let tree = SledTree::open(db, tc.tree_name, true)?;

    let k = 100;

    tree.txn(false, |txn_tree| {
        // sub tree key space
        let nodes_ks = txn_tree.key_space::<Nodes>();

        let _got = nodes_ks.insert(&k, &Node::new("n", Endpoint::new("", 100)))?;

        let got = nodes_ks.get(&k)?;
        assert_eq!(Some(Node::new("n", Endpoint::new("", 100))), got);

        let got = nodes_ks.remove(&k)?;
        assert_eq!(Some(Node::new("n", Endpoint::new("", 100))), got);

        let got = nodes_ks.get(&k)?;
        assert!(got.is_none());

        Ok(())
    })?;

    let got = tree.key_space::<Nodes>().get(&k)?;
    assert!(got.is_none());

    Ok(())
}
