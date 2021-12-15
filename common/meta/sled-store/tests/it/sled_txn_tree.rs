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

use common_base::tokio;
use common_meta_sled_store::SledTree;
use common_meta_sled_store::Store;
use common_meta_types::Node;
use common_tracing::tracing;

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

        let got = nodes_ks.insert(&101, &Node {
            name: "foo".to_string(),
            address: "bar".to_string(),
        })?;

        assert!(got.is_none());

        let got = nodes_ks.insert(&k, &Node {
            name: "n".to_string(),
            address: "a".to_string(),
        })?;

        assert!(got.is_none());

        let got = nodes_ks.get(&k)?;

        assert_eq!(
            Some(Node {
                name: "n".to_string(),
                address: "a".to_string()
            }),
            got
        );

        let got = nodes_ks.insert(&k, &Node {
            name: "m".to_string(),
            address: "c".to_string(),
        })?;

        assert_eq!(
            Some(Node {
                name: "n".to_string(),
                address: "a".to_string()
            }),
            got
        );

        Ok(())
    })?;

    let got = tree.key_space::<Nodes>().get(&k)?;
    assert_eq!(
        Some(Node {
            name: "m".to_string(),
            address: "c".to_string()
        }),
        got
    );

    let got = tree.key_space::<Nodes>().get(&101)?;
    assert_eq!(
        Some(Node {
            name: "foo".to_string(),
            address: "bar".to_string()
        }),
        got
    );

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

        let _got = nodes_ks.insert(&k, &Node {
            name: "n".to_string(),
            address: "a".to_string(),
        })?;

        let got = nodes_ks.get(&k)?;

        assert_eq!(
            Some(Node {
                name: "n".to_string(),
                address: "a".to_string()
            }),
            got
        );

        let got = nodes_ks.remove(&k)?;

        assert_eq!(
            Some(Node {
                name: "n".to_string(),
                address: "a".to_string()
            }),
            got
        );

        let got = nodes_ks.get(&k)?;

        assert!(got.is_none());

        Ok(())
    })?;

    let got = tree.key_space::<Nodes>().get(&k)?;
    assert!(got.is_none());

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_sled_txn_tree_key_space_update_and_fetch() -> anyhow::Result<()> {
    // Test transactional API update_and_fetch on a sub key space of TransactionSledTree

    let (_log_guards, ut_span) = init_sled_ut!();
    let _ent = ut_span.enter();

    let tc = new_sled_test_context();
    let db = &tc.db;
    let tree = SledTree::open(db, tc.tree_name, true)?;

    let k = 100;

    tracing::info!("--- test update default or non-default");

    tree.txn(false, |txn_tree| {
        // sub tree key space
        let nodes_ks = txn_tree.key_space::<Nodes>();

        for _i in 0..3 {
            nodes_ks.update_and_fetch(&k, |old| match old {
                Some(v) => Some(Node {
                    name: v.name + "a",
                    address: v.address,
                }),
                None => Some(Node::default()),
            })?;
        }

        Ok(())
    })?;

    let got = tree.get::<Nodes>(&100)?.unwrap();
    assert_eq!(
        "aa".to_string(),
        got.name,
        "1st time create a default. then append 2 'a'"
    );

    tracing::info!("--- test delete");

    tree.txn(false, |txn_tree| {
        let nodes_ks = txn_tree.key_space::<Nodes>();
        nodes_ks.update_and_fetch(&k, |_old| None)?;

        Ok(())
    })?;

    let got = tree.get::<Nodes>(&100)?;
    assert!(got.is_none(), "delete by return None");
    Ok(())
}
