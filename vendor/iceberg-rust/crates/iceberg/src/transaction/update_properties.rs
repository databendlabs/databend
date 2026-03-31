// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;

use crate::table::Table;
use crate::transaction::action::{ActionCommit, TransactionAction};
use crate::{Error, ErrorKind, Result, TableUpdate};

/// A transactional action that updates or removes table properties
///
/// This action is used to modify key-value pairs in a table's metadata
/// properties during a transaction. It supports setting new values for existing keys
/// or adding new keys, as well as removing existing keys. Each key can only be updated
/// or removed in a single action, not both.
pub struct UpdatePropertiesAction {
    updates: HashMap<String, String>,
    removals: HashSet<String>,
}

impl UpdatePropertiesAction {
    /// Creates a new [`UpdatePropertiesAction`] with no updates or removals.
    pub fn new() -> Self {
        UpdatePropertiesAction {
            updates: HashMap::default(),
            removals: HashSet::default(),
        }
    }

    /// Adds a key-value pair to the update set of this action.
    ///
    /// # Arguments
    ///
    /// * `key` - The property key to update.
    /// * `value` - The new value to associate with the key.
    ///
    /// # Returns
    ///
    /// The updated [`UpdatePropertiesAction`] with the key-value pair added to the update set.
    pub fn set(mut self, key: String, value: String) -> Self {
        self.updates.insert(key, value);
        self
    }

    /// Adds a key to the removal set of this action.
    ///
    /// # Arguments
    ///
    /// * `key` - The property key to remove.
    ///
    /// # Returns
    ///
    /// The updated [`UpdatePropertiesAction`] with the key added to the removal set.
    pub fn remove(mut self, key: String) -> Self {
        self.removals.insert(key);
        self
    }
}

impl Default for UpdatePropertiesAction {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl TransactionAction for UpdatePropertiesAction {
    async fn commit(self: Arc<Self>, _table: &Table) -> Result<ActionCommit> {
        if let Some(overlapping_key) = self.removals.iter().find(|k| self.updates.contains_key(*k))
        {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                format!("Key {overlapping_key} is present in both removal set and update set"),
            ));
        }

        let updates: Vec<TableUpdate> = vec![
            TableUpdate::SetProperties {
                updates: self.updates.clone(),
            },
            TableUpdate::RemoveProperties {
                removals: self.removals.clone().into_iter().collect::<Vec<String>>(),
            },
        ];

        Ok(ActionCommit::new(updates, vec![]))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use as_any::Downcast;

    use crate::transaction::Transaction;
    use crate::transaction::action::ApplyTransactionAction;
    use crate::transaction::tests::make_v2_table;
    use crate::transaction::update_properties::UpdatePropertiesAction;

    #[test]
    fn test_update_table_property() {
        let table = make_v2_table();
        let tx = Transaction::new(&table);
        let tx = tx
            .update_table_properties()
            .set("a".to_string(), "b".to_string())
            .remove("b".to_string())
            .apply(tx)
            .unwrap();

        assert_eq!(tx.actions.len(), 1);

        let action = (*tx.actions[0])
            .downcast_ref::<UpdatePropertiesAction>()
            .unwrap();
        assert_eq!(
            action.updates,
            HashMap::from([("a".to_string(), "b".to_string())])
        );

        assert_eq!(action.removals, HashSet::from(["b".to_string()]));
    }
}
