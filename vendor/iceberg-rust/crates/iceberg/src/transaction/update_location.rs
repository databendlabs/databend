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

use std::sync::Arc;

use async_trait::async_trait;

use crate::table::Table;
use crate::transaction::action::{ActionCommit, TransactionAction};
use crate::{Error, ErrorKind, Result, TableUpdate};

/// A transaction action that sets or updates the location of a table.
///
/// This action is used to explicitly set a new metadata location during a transaction,
/// typically as part of advanced commit or recovery flows. The location is optional until
/// explicitly set via [`set_location`].
pub struct UpdateLocationAction {
    location: Option<String>,
}

impl UpdateLocationAction {
    /// Creates a new [`UpdateLocationAction`] with no location set.
    pub fn new() -> Self {
        UpdateLocationAction { location: None }
    }

    /// Sets the target location for this action and returns the updated instance.
    ///
    /// # Arguments
    ///
    /// * `location` - A string representing the table's location.
    ///
    /// # Returns
    ///
    /// The [`UpdateLocationAction`] with the new location set.
    pub fn set_location(mut self, location: String) -> Self {
        self.location = Some(location);
        self
    }
}

impl Default for UpdateLocationAction {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl TransactionAction for UpdateLocationAction {
    async fn commit(self: Arc<Self>, _table: &Table) -> Result<ActionCommit> {
        let updates: Vec<TableUpdate>;
        if let Some(location) = self.location.clone() {
            updates = vec![TableUpdate::SetLocation { location }];
        } else {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Location is not set for UpdateLocationAction!",
            ));
        }

        Ok(ActionCommit::new(updates, vec![]))
    }
}

#[cfg(test)]
mod tests {
    use as_any::Downcast;

    use crate::transaction::Transaction;
    use crate::transaction::action::ApplyTransactionAction;
    use crate::transaction::tests::make_v2_table;
    use crate::transaction::update_location::UpdateLocationAction;

    #[test]
    fn test_set_location() {
        let table = make_v2_table();
        let tx = Transaction::new(&table);
        let tx = tx
            .update_location()
            .set_location(String::from("s3://bucket/prefix/new_table"))
            .apply(tx)
            .unwrap();

        assert_eq!(tx.actions.len(), 1);

        let action = (*tx.actions[0])
            .downcast_ref::<UpdateLocationAction>()
            .unwrap();

        assert_eq!(
            action.location,
            Some(String::from("s3://bucket/prefix/new_table"))
        )
    }
}
