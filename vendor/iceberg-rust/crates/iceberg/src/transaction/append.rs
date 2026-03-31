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
use uuid::Uuid;

use crate::error::Result;
use crate::spec::{DataFile, ManifestEntry, ManifestFile, Operation};
use crate::table::Table;
use crate::transaction::snapshot::{
    DefaultManifestProcess, SnapshotProduceOperation, SnapshotProducer,
};
use crate::transaction::{ActionCommit, TransactionAction};

/// FastAppendAction is a transaction action for fast append data files to the table.
pub struct FastAppendAction {
    check_duplicate: bool,
    // below are properties used to create SnapshotProducer when commit
    commit_uuid: Option<Uuid>,
    key_metadata: Option<Vec<u8>>,
    snapshot_properties: HashMap<String, String>,
    added_data_files: Vec<DataFile>,
}

impl FastAppendAction {
    pub(crate) fn new() -> Self {
        Self {
            check_duplicate: true,
            commit_uuid: None,
            key_metadata: None,
            snapshot_properties: HashMap::default(),
            added_data_files: vec![],
        }
    }

    /// Set whether to check duplicate files
    pub fn with_check_duplicate(mut self, v: bool) -> Self {
        self.check_duplicate = v;
        self
    }

    /// Add data files to the snapshot.
    pub fn add_data_files(mut self, data_files: impl IntoIterator<Item = DataFile>) -> Self {
        self.added_data_files.extend(data_files);
        self
    }

    /// Set commit UUID for the snapshot.
    pub fn set_commit_uuid(mut self, commit_uuid: Uuid) -> Self {
        self.commit_uuid = Some(commit_uuid);
        self
    }

    /// Set key metadata for manifest files.
    pub fn set_key_metadata(mut self, key_metadata: Vec<u8>) -> Self {
        self.key_metadata = Some(key_metadata);
        self
    }

    /// Set snapshot summary properties.
    pub fn set_snapshot_properties(mut self, snapshot_properties: HashMap<String, String>) -> Self {
        self.snapshot_properties = snapshot_properties;
        self
    }
}

#[async_trait]
impl TransactionAction for FastAppendAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        let snapshot_producer = SnapshotProducer::new(
            table,
            self.commit_uuid.unwrap_or_else(Uuid::now_v7),
            self.key_metadata.clone(),
            self.snapshot_properties.clone(),
            self.added_data_files.clone(),
        );

        // validate added files
        snapshot_producer.validate_added_data_files()?;

        // Checks duplicate files
        if self.check_duplicate {
            snapshot_producer.validate_duplicate_files().await?;
        }

        snapshot_producer
            .commit(FastAppendOperation, DefaultManifestProcess)
            .await
    }
}

struct FastAppendOperation;

impl SnapshotProduceOperation for FastAppendOperation {
    fn operation(&self) -> Operation {
        Operation::Append
    }

    async fn delete_entries(
        &self,
        _snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestEntry>> {
        Ok(vec![])
    }

    async fn existing_manifest(
        &self,
        snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestFile>> {
        let Some(snapshot) = snapshot_produce.table.metadata().current_snapshot() else {
            return Ok(vec![]);
        };

        let manifest_list = snapshot
            .load_manifest_list(
                snapshot_produce.table.file_io(),
                &snapshot_produce.table.metadata_ref(),
            )
            .await?;

        Ok(manifest_list
            .entries()
            .iter()
            .filter(|entry| entry.has_added_files() || entry.has_existing_files())
            .cloned()
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use crate::spec::{
        DataContentType, DataFileBuilder, DataFileFormat, Literal, MAIN_BRANCH, Struct,
    };
    use crate::transaction::tests::make_v2_minimal_table;
    use crate::transaction::{Transaction, TransactionAction};
    use crate::{TableRequirement, TableUpdate};

    #[tokio::test]
    async fn test_empty_data_append_action() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);
        let action = tx.fast_append().add_data_files(vec![]);
        assert!(Arc::new(action).commit(&table).await.is_err());
    }

    #[tokio::test]
    async fn test_set_snapshot_properties() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        let mut snapshot_properties = HashMap::new();
        snapshot_properties.insert("key".to_string(), "val".to_string());

        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap();

        let action = tx
            .fast_append()
            .set_snapshot_properties(snapshot_properties)
            .add_data_files(vec![data_file]);
        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();

        // Check customized properties is contained in snapshot summary properties.
        let new_snapshot = if let TableUpdate::AddSnapshot { snapshot } = &updates[0] {
            snapshot
        } else {
            unreachable!()
        };
        assert_eq!(
            new_snapshot
                .summary()
                .additional_properties
                .get("key")
                .unwrap(),
            "val"
        );
    }

    #[tokio::test]
    async fn test_append_snapshot_properties() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        let mut snapshot_properties = HashMap::new();
        snapshot_properties.insert("key".to_string(), "val".to_string());

        let action = tx
            .fast_append()
            .set_snapshot_properties(snapshot_properties);
        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();

        // Check customized properties is contained in snapshot summary properties.
        let new_snapshot = if let TableUpdate::AddSnapshot { snapshot } = &updates[0] {
            snapshot
        } else {
            unreachable!()
        };
        assert_eq!(
            new_snapshot
                .summary()
                .additional_properties
                .get("key")
                .unwrap(),
            "val"
        );
    }

    #[tokio::test]
    async fn test_fast_append_file_with_incompatible_partition_value() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);
        let action = tx.fast_append();

        // check add data file with incompatible partition value
        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/3.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::string("test"))]))
            .build()
            .unwrap();

        let action = action.add_data_files(vec![data_file.clone()]);

        assert!(Arc::new(action).commit(&table).await.is_err());
    }

    #[tokio::test]
    async fn test_fast_append() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);
        let action = tx.fast_append();

        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/3.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap();

        let action = action.add_data_files(vec![data_file.clone()]);
        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();
        let requirements = action_commit.take_requirements();

        // check updates and requirements
        assert!(
            matches!((&updates[0],&updates[1]), (TableUpdate::AddSnapshot { snapshot },TableUpdate::SetSnapshotRef { reference,ref_name }) if snapshot.snapshot_id() == reference.snapshot_id && ref_name == MAIN_BRANCH)
        );
        assert_eq!(
            vec![
                TableRequirement::UuidMatch {
                    uuid: table.metadata().uuid()
                },
                TableRequirement::RefSnapshotIdMatch {
                    r#ref: MAIN_BRANCH.to_string(),
                    snapshot_id: table.metadata().current_snapshot_id
                }
            ],
            requirements
        );

        // check manifest list
        let new_snapshot = if let TableUpdate::AddSnapshot { snapshot } = &updates[0] {
            snapshot
        } else {
            unreachable!()
        };
        let manifest_list = new_snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        assert_eq!(1, manifest_list.entries().len());
        assert_eq!(
            manifest_list.entries()[0].sequence_number,
            new_snapshot.sequence_number()
        );

        // check manifest
        let manifest = manifest_list.entries()[0]
            .load_manifest(table.file_io())
            .await
            .unwrap();
        assert_eq!(1, manifest.entries().len());
        assert_eq!(
            new_snapshot.sequence_number(),
            manifest.entries()[0]
                .sequence_number()
                .expect("Inherit sequence number by load manifest")
        );

        assert_eq!(
            new_snapshot.snapshot_id(),
            manifest.entries()[0].snapshot_id().unwrap()
        );
        assert_eq!(data_file, *manifest.entries()[0].data_file());
    }
}
