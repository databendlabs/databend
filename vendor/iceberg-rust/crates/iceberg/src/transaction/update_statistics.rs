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

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;

use crate::spec::StatisticsFile;
use crate::table::Table;
use crate::transaction::{ActionCommit, TransactionAction};
use crate::{Result, TableUpdate};

/// A transactional action for updating statistics files in a table
pub struct UpdateStatisticsAction {
    statistics_to_set: HashMap<i64, Option<StatisticsFile>>,
}

impl UpdateStatisticsAction {
    pub fn new() -> Self {
        Self {
            statistics_to_set: HashMap::default(),
        }
    }

    /// Set the table's statistics file for given snapshot, replacing the previous statistics file for
    /// the snapshot if any exists. The snapshot id of the statistics file will be used.
    ///
    /// # Arguments
    ///
    /// * `statistics_file` - The [`StatisticsFile`] to associate with its corresponding snapshot ID.
    ///
    /// # Returns
    ///
    /// An updated [`UpdateStatisticsAction`] with the new statistics file applied.
    pub fn set_statistics(mut self, statistics_file: StatisticsFile) -> Self {
        self.statistics_to_set
            .insert(statistics_file.snapshot_id, Some(statistics_file));
        self
    }

    /// Remove the table's statistics file for given snapshot.
    ///
    /// # Arguments
    ///
    /// * `snapshot_id` - The ID of the snapshot whose statistics file should be removed.
    ///
    /// # Returns
    ///
    /// An updated [`UpdateStatisticsAction`] with the removal operation recorded.
    pub fn remove_statistics(mut self, snapshot_id: i64) -> Self {
        self.statistics_to_set.insert(snapshot_id, None);
        self
    }
}

impl Default for UpdateStatisticsAction {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl TransactionAction for UpdateStatisticsAction {
    async fn commit(self: Arc<Self>, _table: &Table) -> Result<ActionCommit> {
        let mut updates: Vec<TableUpdate> = vec![];

        self.statistics_to_set
            .iter()
            .for_each(|(snapshot_id, statistic_file)| {
                if let Some(statistics) = statistic_file {
                    updates.push(TableUpdate::SetStatistics {
                        statistics: statistics.clone(),
                    })
                } else {
                    updates.push(TableUpdate::RemoveStatistics {
                        snapshot_id: *snapshot_id,
                    })
                }
            });

        Ok(ActionCommit::new(updates, vec![]))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use as_any::Downcast;

    use crate::spec::{BlobMetadata, StatisticsFile};
    use crate::transaction::tests::make_v2_table;
    use crate::transaction::update_statistics::UpdateStatisticsAction;
    use crate::transaction::{ApplyTransactionAction, Transaction};

    #[test]
    fn test_update_statistics() {
        let table = make_v2_table();
        let tx = Transaction::new(&table);

        let statistics_file_1 = StatisticsFile {
            snapshot_id: 3055729675574597004i64,
            statistics_path: "s3://a/b/stats.puffin".to_string(),
            file_size_in_bytes: 413,
            file_footer_size_in_bytes: 42,
            key_metadata: None,
            blob_metadata: vec![BlobMetadata {
                r#type: "ndv".to_string(),
                snapshot_id: 3055729675574597004i64,
                sequence_number: 1,
                fields: vec![1],
                properties: HashMap::new(),
            }],
        };

        let statistics_file_2 = StatisticsFile {
            snapshot_id: 3366729675595277004i64,
            statistics_path: "s3://a/b/stats.puffin".to_string(),
            file_size_in_bytes: 413,
            file_footer_size_in_bytes: 42,
            key_metadata: None,
            blob_metadata: vec![BlobMetadata {
                r#type: "ndv".to_string(),
                snapshot_id: 3366729675595277004i64,
                sequence_number: 1,
                fields: vec![1],
                properties: HashMap::new(),
            }],
        };

        // set stats1
        let tx = tx
            .update_statistics()
            .set_statistics(statistics_file_1.clone())
            .set_statistics(statistics_file_2.clone())
            .remove_statistics(3055729675574597004i64) // remove stats1
            .apply(tx)
            .unwrap();

        let action = (*tx.actions[0])
            .downcast_ref::<UpdateStatisticsAction>()
            .unwrap();
        assert!(
            action
                .statistics_to_set
                .get(&statistics_file_1.snapshot_id)
                .unwrap()
                .is_none()
        ); // stats1 should have been removed
        assert_eq!(
            action
                .statistics_to_set
                .get(&statistics_file_2.snapshot_id)
                .unwrap()
                .clone(),
            Some(statistics_file_2)
        );
    }

    #[test]
    fn test_set_single_statistics() {
        let table = make_v2_table();
        let tx = Transaction::new(&table);

        let statistics_file = StatisticsFile {
            snapshot_id: 1234567890i64,
            statistics_path: "s3://a/b/stats1.puffin".to_string(),
            file_size_in_bytes: 500,
            file_footer_size_in_bytes: 50,
            key_metadata: None,
            blob_metadata: vec![],
        };

        // Set statistics
        let tx = tx
            .update_statistics()
            .set_statistics(statistics_file.clone())
            .apply(tx)
            .unwrap();

        let action = (*tx.actions[0])
            .downcast_ref::<UpdateStatisticsAction>()
            .unwrap();

        // Verify that the statistics file is set correctly
        assert_eq!(
            action
                .statistics_to_set
                .get(&statistics_file.snapshot_id)
                .unwrap()
                .clone(),
            Some(statistics_file)
        );
    }

    #[test]
    fn test_no_statistics_set() {
        let table = make_v2_table();
        let tx = Transaction::new(&table);

        // No statistics are set or removed
        let tx = tx.update_statistics().apply(tx).unwrap();

        let action = (*tx.actions[0])
            .downcast_ref::<UpdateStatisticsAction>()
            .unwrap();

        // Verify that no statistics are set
        assert!(action.statistics_to_set.is_empty());
    }
}
