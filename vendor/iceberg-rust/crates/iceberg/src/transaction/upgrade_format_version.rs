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

use crate::TableUpdate::UpgradeFormatVersion;
use crate::spec::FormatVersion;
use crate::table::Table;
use crate::transaction::action::{ActionCommit, TransactionAction};
use crate::{Error, ErrorKind, Result};

/// A transaction action to upgrade a table's format version.
///
/// This action is used within a transaction to indicate that the
/// table's format version should be upgraded to a specified version.
/// The location remains optional until explicitly set via [`set_format_version`].
pub struct UpgradeFormatVersionAction {
    format_version: Option<FormatVersion>,
}

impl UpgradeFormatVersionAction {
    /// Creates a new `UpgradeFormatVersionAction` with no version set.
    pub fn new() -> Self {
        UpgradeFormatVersionAction {
            format_version: None,
        }
    }

    /// Sets the target format version for the upgrade.
    ///
    /// # Arguments
    ///
    /// * `format_version` - The version to upgrade the table format to.
    ///
    /// # Returns
    ///
    /// Returns the updated `UpgradeFormatVersionAction` with the format version set.
    pub fn set_format_version(mut self, format_version: FormatVersion) -> Self {
        self.format_version = Some(format_version);
        self
    }
}

impl Default for UpgradeFormatVersionAction {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl TransactionAction for UpgradeFormatVersionAction {
    async fn commit(self: Arc<Self>, _table: &Table) -> Result<ActionCommit> {
        let format_version = self.format_version.ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                "FormatVersion is not set for UpgradeFormatVersionAction!",
            )
        })?;

        Ok(ActionCommit::new(
            vec![UpgradeFormatVersion { format_version }],
            vec![],
        ))
    }
}

#[cfg(test)]
mod tests {
    use as_any::Downcast;

    use crate::spec::FormatVersion;
    use crate::transaction::Transaction;
    use crate::transaction::action::ApplyTransactionAction;
    use crate::transaction::upgrade_format_version::UpgradeFormatVersionAction;

    #[test]
    fn test_upgrade_format_version() {
        let table = crate::transaction::tests::make_v1_table();
        let tx = Transaction::new(&table);
        let tx = tx
            .upgrade_table_version()
            .set_format_version(FormatVersion::V2)
            .apply(tx)
            .unwrap();

        assert_eq!(tx.actions.len(), 1);

        let action = (*tx.actions[0])
            .downcast_ref::<UpgradeFormatVersionAction>()
            .unwrap();

        assert_eq!(action.format_version, Some(FormatVersion::V2));
    }
}
