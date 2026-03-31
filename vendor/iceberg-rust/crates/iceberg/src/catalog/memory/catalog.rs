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

//! This module contains memory catalog implementation.

use std::collections::HashMap;

use async_trait::async_trait;
use futures::lock::{Mutex, MutexGuard};
use itertools::Itertools;

use super::namespace_state::NamespaceState;
use crate::io::FileIO;
use crate::spec::{TableMetadata, TableMetadataBuilder};
use crate::table::Table;
use crate::{
    Catalog, CatalogBuilder, Error, ErrorKind, MetadataLocation, Namespace, NamespaceIdent, Result,
    TableCommit, TableCreation, TableIdent,
};

/// Memory catalog warehouse location
pub const MEMORY_CATALOG_WAREHOUSE: &str = "warehouse";

/// namespace `location` property
const LOCATION: &str = "location";

/// Builder for [`MemoryCatalog`].
#[derive(Debug)]
pub struct MemoryCatalogBuilder(MemoryCatalogConfig);

impl Default for MemoryCatalogBuilder {
    fn default() -> Self {
        Self(MemoryCatalogConfig {
            name: None,
            warehouse: "".to_string(),
            props: HashMap::new(),
        })
    }
}

impl CatalogBuilder for MemoryCatalogBuilder {
    type C = MemoryCatalog;

    fn load(
        mut self,
        name: impl Into<String>,
        props: HashMap<String, String>,
    ) -> impl Future<Output = Result<Self::C>> + Send {
        self.0.name = Some(name.into());

        if props.contains_key(MEMORY_CATALOG_WAREHOUSE) {
            self.0.warehouse = props
                .get(MEMORY_CATALOG_WAREHOUSE)
                .cloned()
                .unwrap_or_default()
        }

        // Collect other remaining properties
        self.0.props = props
            .into_iter()
            .filter(|(k, _)| k != MEMORY_CATALOG_WAREHOUSE)
            .collect();

        let result = {
            if self.0.name.is_none() {
                Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Catalog name is required",
                ))
            } else if self.0.warehouse.is_empty() {
                Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Catalog warehouse is required",
                ))
            } else {
                MemoryCatalog::new(self.0)
            }
        };

        std::future::ready(result)
    }
}

#[derive(Clone, Debug)]
pub(crate) struct MemoryCatalogConfig {
    name: Option<String>,
    warehouse: String,
    props: HashMap<String, String>,
}

/// Memory catalog implementation.
#[derive(Debug)]
pub struct MemoryCatalog {
    root_namespace_state: Mutex<NamespaceState>,
    file_io: FileIO,
    warehouse_location: String,
}

impl MemoryCatalog {
    /// Creates a memory catalog.
    fn new(config: MemoryCatalogConfig) -> Result<Self> {
        Ok(Self {
            root_namespace_state: Mutex::new(NamespaceState::default()),
            file_io: FileIO::from_path(&config.warehouse)?
                .with_props(config.props)
                .build()?,
            warehouse_location: config.warehouse,
        })
    }

    /// Loads a table from the locked namespace state.
    async fn load_table_from_locked_state(
        &self,
        table_ident: &TableIdent,
        root_namespace_state: &MutexGuard<'_, NamespaceState>,
    ) -> Result<Table> {
        let metadata_location = root_namespace_state.get_existing_table_location(table_ident)?;
        let metadata = TableMetadata::read_from(&self.file_io, metadata_location).await?;

        Table::builder()
            .identifier(table_ident.clone())
            .metadata(metadata)
            .metadata_location(metadata_location.to_string())
            .file_io(self.file_io.clone())
            .build()
    }
}

#[async_trait]
impl Catalog for MemoryCatalog {
    /// List namespaces inside the catalog.
    async fn list_namespaces(
        &self,
        maybe_parent: Option<&NamespaceIdent>,
    ) -> Result<Vec<NamespaceIdent>> {
        let root_namespace_state = self.root_namespace_state.lock().await;

        match maybe_parent {
            None => {
                let namespaces = root_namespace_state
                    .list_top_level_namespaces()
                    .into_iter()
                    .map(|str| NamespaceIdent::new(str.to_string()))
                    .collect_vec();

                Ok(namespaces)
            }
            Some(parent_namespace_ident) => {
                let namespaces = root_namespace_state
                    .list_namespaces_under(parent_namespace_ident)?
                    .into_iter()
                    .map(|name| {
                        let mut names = parent_namespace_ident.iter().cloned().collect::<Vec<_>>();
                        names.push(name.to_string());
                        NamespaceIdent::from_vec(names)
                    })
                    .collect::<Result<Vec<_>>>()?;

                Ok(namespaces)
            }
        }
    }

    /// Create a new namespace inside the catalog.
    async fn create_namespace(
        &self,
        namespace_ident: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<Namespace> {
        let mut root_namespace_state = self.root_namespace_state.lock().await;

        root_namespace_state.insert_new_namespace(namespace_ident, properties.clone())?;
        let namespace = Namespace::with_properties(namespace_ident.clone(), properties);

        Ok(namespace)
    }

    /// Get a namespace information from the catalog.
    async fn get_namespace(&self, namespace_ident: &NamespaceIdent) -> Result<Namespace> {
        let root_namespace_state = self.root_namespace_state.lock().await;

        let namespace = Namespace::with_properties(
            namespace_ident.clone(),
            root_namespace_state
                .get_properties(namespace_ident)?
                .clone(),
        );

        Ok(namespace)
    }

    /// Check if namespace exists in catalog.
    async fn namespace_exists(&self, namespace_ident: &NamespaceIdent) -> Result<bool> {
        let guarded_namespaces = self.root_namespace_state.lock().await;

        Ok(guarded_namespaces.namespace_exists(namespace_ident))
    }

    /// Update a namespace inside the catalog.
    ///
    /// # Behavior
    ///
    /// The properties must be the full set of namespace.
    async fn update_namespace(
        &self,
        namespace_ident: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<()> {
        let mut root_namespace_state = self.root_namespace_state.lock().await;

        root_namespace_state.replace_properties(namespace_ident, properties)
    }

    /// Drop a namespace from the catalog.
    async fn drop_namespace(&self, namespace_ident: &NamespaceIdent) -> Result<()> {
        let mut root_namespace_state = self.root_namespace_state.lock().await;

        root_namespace_state.remove_existing_namespace(namespace_ident)
    }

    /// List tables from namespace.
    async fn list_tables(&self, namespace_ident: &NamespaceIdent) -> Result<Vec<TableIdent>> {
        let root_namespace_state = self.root_namespace_state.lock().await;

        let table_names = root_namespace_state.list_tables(namespace_ident)?;
        let table_idents = table_names
            .into_iter()
            .map(|table_name| TableIdent::new(namespace_ident.clone(), table_name.clone()))
            .collect_vec();

        Ok(table_idents)
    }

    /// Create a new table inside the namespace.
    async fn create_table(
        &self,
        namespace_ident: &NamespaceIdent,
        table_creation: TableCreation,
    ) -> Result<Table> {
        let mut root_namespace_state = self.root_namespace_state.lock().await;

        let table_name = table_creation.name.clone();
        let table_ident = TableIdent::new(namespace_ident.clone(), table_name);

        let (table_creation, location) = match table_creation.location.clone() {
            Some(location) => (table_creation, location),
            None => {
                let namespace_properties = root_namespace_state.get_properties(namespace_ident)?;
                let location_prefix = match namespace_properties.get(LOCATION) {
                    Some(namespace_location) => namespace_location.clone(),
                    None => format!("{}/{}", self.warehouse_location, namespace_ident.join("/")),
                };

                let location = format!("{}/{}", location_prefix, table_ident.name());

                let new_table_creation = TableCreation {
                    location: Some(location.clone()),
                    ..table_creation
                };

                (new_table_creation, location)
            }
        };

        let metadata = TableMetadataBuilder::from_table_creation(table_creation)?
            .build()?
            .metadata;
        let metadata_location = MetadataLocation::new_with_table_location(location).to_string();

        metadata.write_to(&self.file_io, &metadata_location).await?;

        root_namespace_state.insert_new_table(&table_ident, metadata_location.clone())?;

        Table::builder()
            .file_io(self.file_io.clone())
            .metadata_location(metadata_location)
            .metadata(metadata)
            .identifier(table_ident)
            .build()
    }

    /// Load table from the catalog.
    async fn load_table(&self, table_ident: &TableIdent) -> Result<Table> {
        let root_namespace_state = self.root_namespace_state.lock().await;

        self.load_table_from_locked_state(table_ident, &root_namespace_state)
            .await
    }

    /// Drop a table from the catalog.
    async fn drop_table(&self, table_ident: &TableIdent) -> Result<()> {
        let mut root_namespace_state = self.root_namespace_state.lock().await;

        let metadata_location = root_namespace_state.remove_existing_table(table_ident)?;
        self.file_io.delete(&metadata_location).await
    }

    /// Check if a table exists in the catalog.
    async fn table_exists(&self, table_ident: &TableIdent) -> Result<bool> {
        let root_namespace_state = self.root_namespace_state.lock().await;

        root_namespace_state.table_exists(table_ident)
    }

    /// Rename a table in the catalog.
    async fn rename_table(
        &self,
        src_table_ident: &TableIdent,
        dst_table_ident: &TableIdent,
    ) -> Result<()> {
        let mut root_namespace_state = self.root_namespace_state.lock().await;

        let mut new_root_namespace_state = root_namespace_state.clone();
        let metadata_location = new_root_namespace_state
            .get_existing_table_location(src_table_ident)?
            .clone();
        new_root_namespace_state.remove_existing_table(src_table_ident)?;
        new_root_namespace_state.insert_new_table(dst_table_ident, metadata_location)?;
        *root_namespace_state = new_root_namespace_state;

        Ok(())
    }

    async fn register_table(
        &self,
        table_ident: &TableIdent,
        metadata_location: String,
    ) -> Result<Table> {
        let mut root_namespace_state = self.root_namespace_state.lock().await;
        root_namespace_state.insert_new_table(&table_ident.clone(), metadata_location.clone())?;

        let metadata = TableMetadata::read_from(&self.file_io, &metadata_location).await?;

        Table::builder()
            .file_io(self.file_io.clone())
            .metadata_location(metadata_location)
            .metadata(metadata)
            .identifier(table_ident.clone())
            .build()
    }

    /// Update a table in the catalog.
    async fn update_table(&self, commit: TableCommit) -> Result<Table> {
        let mut root_namespace_state = self.root_namespace_state.lock().await;

        let current_table = self
            .load_table_from_locked_state(commit.identifier(), &root_namespace_state)
            .await?;

        // Apply TableCommit to get staged table
        let staged_table = commit.apply(current_table)?;

        // Write table metadata to the new location
        staged_table
            .metadata()
            .write_to(
                staged_table.file_io(),
                staged_table.metadata_location_result()?,
            )
            .await?;

        // Flip the pointer to reference the new metadata file.
        let updated_table = root_namespace_state.commit_table_update(staged_table)?;

        Ok(updated_table)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::collections::HashSet;
    use std::hash::Hash;
    use std::iter::FromIterator;
    use std::vec;

    use regex::Regex;
    use tempfile::TempDir;

    use super::*;
    use crate::io::FileIOBuilder;
    use crate::spec::{NestedField, PartitionSpec, PrimitiveType, Schema, SortOrder, Type};
    use crate::transaction::{ApplyTransactionAction, Transaction};

    fn temp_path() -> String {
        let temp_dir = TempDir::new().unwrap();
        temp_dir.path().to_str().unwrap().to_string()
    }

    pub(crate) async fn new_memory_catalog() -> impl Catalog {
        let warehouse_location = temp_path();
        MemoryCatalogBuilder::default()
            .load(
                "memory",
                HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse_location)]),
            )
            .await
            .unwrap()
    }

    async fn create_namespace<C: Catalog>(catalog: &C, namespace_ident: &NamespaceIdent) {
        let _ = catalog
            .create_namespace(namespace_ident, HashMap::new())
            .await
            .unwrap();
    }

    async fn create_namespaces<C: Catalog>(catalog: &C, namespace_idents: &Vec<&NamespaceIdent>) {
        for namespace_ident in namespace_idents {
            let _ = create_namespace(catalog, namespace_ident).await;
        }
    }

    fn to_set<T: Eq + Hash>(vec: Vec<T>) -> HashSet<T> {
        HashSet::from_iter(vec)
    }

    fn simple_table_schema() -> Schema {
        Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "foo", Type::Primitive(PrimitiveType::Int)).into(),
            ])
            .build()
            .unwrap()
    }

    async fn create_table<C: Catalog>(catalog: &C, table_ident: &TableIdent) -> Table {
        catalog
            .create_table(
                &table_ident.namespace,
                TableCreation::builder()
                    .name(table_ident.name().into())
                    .schema(simple_table_schema())
                    .build(),
            )
            .await
            .unwrap()
    }

    async fn create_tables<C: Catalog>(catalog: &C, table_idents: Vec<&TableIdent>) {
        for table_ident in table_idents {
            create_table(catalog, table_ident).await;
        }
    }

    async fn create_table_with_namespace<C: Catalog>(catalog: &C) -> Table {
        let namespace_ident = NamespaceIdent::new("abc".into());
        create_namespace(catalog, &namespace_ident).await;

        let table_ident = TableIdent::new(namespace_ident, "test".to_string());
        create_table(catalog, &table_ident).await
    }

    fn assert_table_eq(table: &Table, expected_table_ident: &TableIdent, expected_schema: &Schema) {
        assert_eq!(table.identifier(), expected_table_ident);

        let metadata = table.metadata();

        assert_eq!(metadata.current_schema().as_ref(), expected_schema);

        let expected_partition_spec = PartitionSpec::builder((*expected_schema).clone())
            .with_spec_id(0)
            .build()
            .unwrap();

        assert_eq!(
            metadata
                .partition_specs_iter()
                .map(|p| p.as_ref())
                .collect_vec(),
            vec![&expected_partition_spec]
        );

        let expected_sorted_order = SortOrder::builder()
            .with_order_id(0)
            .with_fields(vec![])
            .build(expected_schema)
            .unwrap();

        assert_eq!(
            metadata
                .sort_orders_iter()
                .map(|s| s.as_ref())
                .collect_vec(),
            vec![&expected_sorted_order]
        );

        assert_eq!(metadata.properties(), &HashMap::new());

        assert!(!table.readonly());
    }

    const UUID_REGEX_STR: &str = "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}";

    fn assert_table_metadata_location_matches(table: &Table, regex_str: &str) {
        let actual = table.metadata_location().unwrap().to_string();
        let regex = Regex::new(regex_str).unwrap();
        assert!(
            regex.is_match(&actual),
            "Expected metadata location to match regex, but got location: {actual} and regex: {regex}"
        )
    }

    #[tokio::test]
    async fn test_list_namespaces_returns_empty_vector() {
        let catalog = new_memory_catalog().await;

        assert_eq!(catalog.list_namespaces(None).await.unwrap(), vec![]);
    }

    #[tokio::test]
    async fn test_list_namespaces_returns_single_namespace() {
        let catalog = new_memory_catalog().await;
        let namespace_ident = NamespaceIdent::new("abc".into());
        create_namespace(&catalog, &namespace_ident).await;

        assert_eq!(catalog.list_namespaces(None).await.unwrap(), vec![
            namespace_ident
        ]);
    }

    #[tokio::test]
    async fn test_list_namespaces_returns_multiple_namespaces() {
        let catalog = new_memory_catalog().await;
        let namespace_ident_1 = NamespaceIdent::new("a".into());
        let namespace_ident_2 = NamespaceIdent::new("b".into());
        create_namespaces(&catalog, &vec![&namespace_ident_1, &namespace_ident_2]).await;

        assert_eq!(
            to_set(catalog.list_namespaces(None).await.unwrap()),
            to_set(vec![namespace_ident_1, namespace_ident_2])
        );
    }

    #[tokio::test]
    async fn test_list_namespaces_returns_only_top_level_namespaces() {
        let catalog = new_memory_catalog().await;
        let namespace_ident_1 = NamespaceIdent::new("a".into());
        let namespace_ident_2 = NamespaceIdent::from_strs(vec!["a", "b"]).unwrap();
        let namespace_ident_3 = NamespaceIdent::new("b".into());
        create_namespaces(&catalog, &vec![
            &namespace_ident_1,
            &namespace_ident_2,
            &namespace_ident_3,
        ])
        .await;

        assert_eq!(
            to_set(catalog.list_namespaces(None).await.unwrap()),
            to_set(vec![namespace_ident_1, namespace_ident_3])
        );
    }

    #[tokio::test]
    async fn test_list_namespaces_returns_no_namespaces_under_parent() {
        let catalog = new_memory_catalog().await;
        let namespace_ident_1 = NamespaceIdent::new("a".into());
        let namespace_ident_2 = NamespaceIdent::new("b".into());
        create_namespaces(&catalog, &vec![&namespace_ident_1, &namespace_ident_2]).await;

        assert_eq!(
            catalog
                .list_namespaces(Some(&namespace_ident_1))
                .await
                .unwrap(),
            vec![]
        );
    }

    #[tokio::test]
    async fn test_list_namespaces_returns_namespace_under_parent() {
        let catalog = new_memory_catalog().await;
        let namespace_ident_1 = NamespaceIdent::new("a".into());
        let namespace_ident_2 = NamespaceIdent::from_strs(vec!["a", "b"]).unwrap();
        let namespace_ident_3 = NamespaceIdent::new("c".into());
        create_namespaces(&catalog, &vec![
            &namespace_ident_1,
            &namespace_ident_2,
            &namespace_ident_3,
        ])
        .await;

        assert_eq!(
            to_set(catalog.list_namespaces(None).await.unwrap()),
            to_set(vec![namespace_ident_1.clone(), namespace_ident_3])
        );

        assert_eq!(
            catalog
                .list_namespaces(Some(&namespace_ident_1))
                .await
                .unwrap(),
            vec![namespace_ident_2]
        );
    }

    #[tokio::test]
    async fn test_list_namespaces_returns_multiple_namespaces_under_parent() {
        let catalog = new_memory_catalog().await;
        let namespace_ident_1 = NamespaceIdent::new("a".to_string());
        let namespace_ident_2 = NamespaceIdent::from_strs(vec!["a", "a"]).unwrap();
        let namespace_ident_3 = NamespaceIdent::from_strs(vec!["a", "b"]).unwrap();
        let namespace_ident_4 = NamespaceIdent::from_strs(vec!["a", "c"]).unwrap();
        let namespace_ident_5 = NamespaceIdent::new("b".into());
        create_namespaces(&catalog, &vec![
            &namespace_ident_1,
            &namespace_ident_2,
            &namespace_ident_3,
            &namespace_ident_4,
            &namespace_ident_5,
        ])
        .await;

        assert_eq!(
            to_set(
                catalog
                    .list_namespaces(Some(&namespace_ident_1))
                    .await
                    .unwrap()
            ),
            to_set(vec![
                namespace_ident_2,
                namespace_ident_3,
                namespace_ident_4,
            ])
        );
    }

    #[tokio::test]
    async fn test_namespace_exists_returns_false() {
        let catalog = new_memory_catalog().await;
        let namespace_ident = NamespaceIdent::new("a".into());
        create_namespace(&catalog, &namespace_ident).await;

        assert!(
            !catalog
                .namespace_exists(&NamespaceIdent::new("b".into()))
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn test_namespace_exists_returns_true() {
        let catalog = new_memory_catalog().await;
        let namespace_ident = NamespaceIdent::new("a".into());
        create_namespace(&catalog, &namespace_ident).await;

        assert!(catalog.namespace_exists(&namespace_ident).await.unwrap());
    }

    #[tokio::test]
    async fn test_create_namespace_with_empty_properties() {
        let catalog = new_memory_catalog().await;
        let namespace_ident = NamespaceIdent::new("a".into());

        assert_eq!(
            catalog
                .create_namespace(&namespace_ident, HashMap::new())
                .await
                .unwrap(),
            Namespace::new(namespace_ident.clone())
        );

        assert_eq!(
            catalog.get_namespace(&namespace_ident).await.unwrap(),
            Namespace::with_properties(namespace_ident, HashMap::new())
        );
    }

    #[tokio::test]
    async fn test_create_namespace_with_properties() {
        let catalog = new_memory_catalog().await;
        let namespace_ident = NamespaceIdent::new("abc".into());

        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert("k".into(), "v".into());

        assert_eq!(
            catalog
                .create_namespace(&namespace_ident, properties.clone())
                .await
                .unwrap(),
            Namespace::with_properties(namespace_ident.clone(), properties.clone())
        );

        assert_eq!(
            catalog.get_namespace(&namespace_ident).await.unwrap(),
            Namespace::with_properties(namespace_ident, properties)
        );
    }

    #[tokio::test]
    async fn test_create_namespace_throws_error_if_namespace_already_exists() {
        let catalog = new_memory_catalog().await;
        let namespace_ident = NamespaceIdent::new("a".into());
        create_namespace(&catalog, &namespace_ident).await;

        assert_eq!(
            catalog
                .create_namespace(&namespace_ident, HashMap::new())
                .await
                .unwrap_err()
                .to_string(),
            format!(
                "NamespaceAlreadyExists => Cannot create namespace {:?}. Namespace already exists.",
                &namespace_ident
            )
        );

        assert_eq!(
            catalog.get_namespace(&namespace_ident).await.unwrap(),
            Namespace::with_properties(namespace_ident, HashMap::new())
        );
    }

    #[tokio::test]
    async fn test_create_nested_namespace() {
        let catalog = new_memory_catalog().await;
        let parent_namespace_ident = NamespaceIdent::new("a".into());
        create_namespace(&catalog, &parent_namespace_ident).await;

        let child_namespace_ident = NamespaceIdent::from_strs(vec!["a", "b"]).unwrap();

        assert_eq!(
            catalog
                .create_namespace(&child_namespace_ident, HashMap::new())
                .await
                .unwrap(),
            Namespace::new(child_namespace_ident.clone())
        );

        assert_eq!(
            catalog.get_namespace(&child_namespace_ident).await.unwrap(),
            Namespace::with_properties(child_namespace_ident, HashMap::new())
        );
    }

    #[tokio::test]
    async fn test_create_deeply_nested_namespace() {
        let catalog = new_memory_catalog().await;
        let namespace_ident_a = NamespaceIdent::new("a".into());
        let namespace_ident_a_b = NamespaceIdent::from_strs(vec!["a", "b"]).unwrap();
        create_namespaces(&catalog, &vec![&namespace_ident_a, &namespace_ident_a_b]).await;

        let namespace_ident_a_b_c = NamespaceIdent::from_strs(vec!["a", "b", "c"]).unwrap();

        assert_eq!(
            catalog
                .create_namespace(&namespace_ident_a_b_c, HashMap::new())
                .await
                .unwrap(),
            Namespace::new(namespace_ident_a_b_c.clone())
        );

        assert_eq!(
            catalog.get_namespace(&namespace_ident_a_b_c).await.unwrap(),
            Namespace::with_properties(namespace_ident_a_b_c, HashMap::new())
        );
    }

    #[tokio::test]
    async fn test_create_nested_namespace_throws_error_if_top_level_namespace_doesnt_exist() {
        let catalog = new_memory_catalog().await;

        let nested_namespace_ident = NamespaceIdent::from_strs(vec!["a", "b"]).unwrap();

        assert_eq!(
            catalog
                .create_namespace(&nested_namespace_ident, HashMap::new())
                .await
                .unwrap_err()
                .to_string(),
            format!(
                "NamespaceNotFound => No such namespace: {:?}",
                NamespaceIdent::new("a".into())
            )
        );

        assert_eq!(catalog.list_namespaces(None).await.unwrap(), vec![]);
    }

    #[tokio::test]
    async fn test_create_deeply_nested_namespace_throws_error_if_intermediate_namespace_doesnt_exist()
     {
        let catalog = new_memory_catalog().await;

        let namespace_ident_a = NamespaceIdent::new("a".into());
        create_namespace(&catalog, &namespace_ident_a).await;

        let namespace_ident_a_b_c = NamespaceIdent::from_strs(vec!["a", "b", "c"]).unwrap();

        assert_eq!(
            catalog
                .create_namespace(&namespace_ident_a_b_c, HashMap::new())
                .await
                .unwrap_err()
                .to_string(),
            format!(
                "NamespaceNotFound => No such namespace: {:?}",
                NamespaceIdent::from_strs(vec!["a", "b"]).unwrap()
            )
        );

        assert_eq!(catalog.list_namespaces(None).await.unwrap(), vec![
            namespace_ident_a.clone()
        ]);

        assert_eq!(
            catalog
                .list_namespaces(Some(&namespace_ident_a))
                .await
                .unwrap(),
            vec![]
        );
    }

    #[tokio::test]
    async fn test_get_namespace() {
        let catalog = new_memory_catalog().await;
        let namespace_ident = NamespaceIdent::new("abc".into());

        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert("k".into(), "v".into());
        let _ = catalog
            .create_namespace(&namespace_ident, properties.clone())
            .await
            .unwrap();

        assert_eq!(
            catalog.get_namespace(&namespace_ident).await.unwrap(),
            Namespace::with_properties(namespace_ident, properties)
        )
    }

    #[tokio::test]
    async fn test_get_nested_namespace() {
        let catalog = new_memory_catalog().await;
        let namespace_ident_a = NamespaceIdent::new("a".into());
        let namespace_ident_a_b = NamespaceIdent::from_strs(vec!["a", "b"]).unwrap();
        create_namespaces(&catalog, &vec![&namespace_ident_a, &namespace_ident_a_b]).await;

        assert_eq!(
            catalog.get_namespace(&namespace_ident_a_b).await.unwrap(),
            Namespace::with_properties(namespace_ident_a_b, HashMap::new())
        );
    }

    #[tokio::test]
    async fn test_get_deeply_nested_namespace() {
        let catalog = new_memory_catalog().await;
        let namespace_ident_a = NamespaceIdent::new("a".into());
        let namespace_ident_a_b = NamespaceIdent::from_strs(vec!["a", "b"]).unwrap();
        let namespace_ident_a_b_c = NamespaceIdent::from_strs(vec!["a", "b", "c"]).unwrap();
        create_namespaces(&catalog, &vec![
            &namespace_ident_a,
            &namespace_ident_a_b,
            &namespace_ident_a_b_c,
        ])
        .await;

        assert_eq!(
            catalog.get_namespace(&namespace_ident_a_b_c).await.unwrap(),
            Namespace::with_properties(namespace_ident_a_b_c, HashMap::new())
        );
    }

    #[tokio::test]
    async fn test_get_namespace_throws_error_if_namespace_doesnt_exist() {
        let catalog = new_memory_catalog().await;
        create_namespace(&catalog, &NamespaceIdent::new("a".into())).await;

        let non_existent_namespace_ident = NamespaceIdent::new("b".into());
        assert_eq!(
            catalog
                .get_namespace(&non_existent_namespace_ident)
                .await
                .unwrap_err()
                .to_string(),
            format!("NamespaceNotFound => No such namespace: {non_existent_namespace_ident:?}")
        )
    }

    #[tokio::test]
    async fn test_update_namespace() {
        let catalog = new_memory_catalog().await;
        let namespace_ident = NamespaceIdent::new("abc".into());
        create_namespace(&catalog, &namespace_ident).await;

        let mut new_properties: HashMap<String, String> = HashMap::new();
        new_properties.insert("k".into(), "v".into());

        catalog
            .update_namespace(&namespace_ident, new_properties.clone())
            .await
            .unwrap();

        assert_eq!(
            catalog.get_namespace(&namespace_ident).await.unwrap(),
            Namespace::with_properties(namespace_ident, new_properties)
        )
    }

    #[tokio::test]
    async fn test_update_nested_namespace() {
        let catalog = new_memory_catalog().await;
        let namespace_ident_a = NamespaceIdent::new("a".into());
        let namespace_ident_a_b = NamespaceIdent::from_strs(vec!["a", "b"]).unwrap();
        create_namespaces(&catalog, &vec![&namespace_ident_a, &namespace_ident_a_b]).await;

        let mut new_properties = HashMap::new();
        new_properties.insert("k".into(), "v".into());

        catalog
            .update_namespace(&namespace_ident_a_b, new_properties.clone())
            .await
            .unwrap();

        assert_eq!(
            catalog.get_namespace(&namespace_ident_a_b).await.unwrap(),
            Namespace::with_properties(namespace_ident_a_b, new_properties)
        );
    }

    #[tokio::test]
    async fn test_update_deeply_nested_namespace() {
        let catalog = new_memory_catalog().await;
        let namespace_ident_a = NamespaceIdent::new("a".into());
        let namespace_ident_a_b = NamespaceIdent::from_strs(vec!["a", "b"]).unwrap();
        let namespace_ident_a_b_c = NamespaceIdent::from_strs(vec!["a", "b", "c"]).unwrap();
        create_namespaces(&catalog, &vec![
            &namespace_ident_a,
            &namespace_ident_a_b,
            &namespace_ident_a_b_c,
        ])
        .await;

        let mut new_properties = HashMap::new();
        new_properties.insert("k".into(), "v".into());

        catalog
            .update_namespace(&namespace_ident_a_b_c, new_properties.clone())
            .await
            .unwrap();

        assert_eq!(
            catalog.get_namespace(&namespace_ident_a_b_c).await.unwrap(),
            Namespace::with_properties(namespace_ident_a_b_c, new_properties)
        );
    }

    #[tokio::test]
    async fn test_update_namespace_throws_error_if_namespace_doesnt_exist() {
        let catalog = new_memory_catalog().await;
        create_namespace(&catalog, &NamespaceIdent::new("abc".into())).await;

        let non_existent_namespace_ident = NamespaceIdent::new("def".into());
        assert_eq!(
            catalog
                .update_namespace(&non_existent_namespace_ident, HashMap::new())
                .await
                .unwrap_err()
                .to_string(),
            format!("NamespaceNotFound => No such namespace: {non_existent_namespace_ident:?}")
        )
    }

    #[tokio::test]
    async fn test_drop_namespace() {
        let catalog = new_memory_catalog().await;
        let namespace_ident = NamespaceIdent::new("abc".into());
        create_namespace(&catalog, &namespace_ident).await;

        catalog.drop_namespace(&namespace_ident).await.unwrap();

        assert!(!catalog.namespace_exists(&namespace_ident).await.unwrap())
    }

    #[tokio::test]
    async fn test_drop_nested_namespace() {
        let catalog = new_memory_catalog().await;
        let namespace_ident_a = NamespaceIdent::new("a".into());
        let namespace_ident_a_b = NamespaceIdent::from_strs(vec!["a", "b"]).unwrap();
        create_namespaces(&catalog, &vec![&namespace_ident_a, &namespace_ident_a_b]).await;

        catalog.drop_namespace(&namespace_ident_a_b).await.unwrap();

        assert!(
            !catalog
                .namespace_exists(&namespace_ident_a_b)
                .await
                .unwrap()
        );

        assert!(catalog.namespace_exists(&namespace_ident_a).await.unwrap());
    }

    #[tokio::test]
    async fn test_drop_deeply_nested_namespace() {
        let catalog = new_memory_catalog().await;
        let namespace_ident_a = NamespaceIdent::new("a".into());
        let namespace_ident_a_b = NamespaceIdent::from_strs(vec!["a", "b"]).unwrap();
        let namespace_ident_a_b_c = NamespaceIdent::from_strs(vec!["a", "b", "c"]).unwrap();
        create_namespaces(&catalog, &vec![
            &namespace_ident_a,
            &namespace_ident_a_b,
            &namespace_ident_a_b_c,
        ])
        .await;

        catalog
            .drop_namespace(&namespace_ident_a_b_c)
            .await
            .unwrap();

        assert!(
            !catalog
                .namespace_exists(&namespace_ident_a_b_c)
                .await
                .unwrap()
        );

        assert!(
            catalog
                .namespace_exists(&namespace_ident_a_b)
                .await
                .unwrap()
        );

        assert!(catalog.namespace_exists(&namespace_ident_a).await.unwrap());
    }

    #[tokio::test]
    async fn test_drop_namespace_throws_error_if_namespace_doesnt_exist() {
        let catalog = new_memory_catalog().await;

        let non_existent_namespace_ident = NamespaceIdent::new("abc".into());
        assert_eq!(
            catalog
                .drop_namespace(&non_existent_namespace_ident)
                .await
                .unwrap_err()
                .to_string(),
            format!("NamespaceNotFound => No such namespace: {non_existent_namespace_ident:?}")
        )
    }

    #[tokio::test]
    async fn test_drop_namespace_throws_error_if_nested_namespace_doesnt_exist() {
        let catalog = new_memory_catalog().await;
        create_namespace(&catalog, &NamespaceIdent::new("a".into())).await;

        let non_existent_namespace_ident =
            NamespaceIdent::from_vec(vec!["a".into(), "b".into()]).unwrap();
        assert_eq!(
            catalog
                .drop_namespace(&non_existent_namespace_ident)
                .await
                .unwrap_err()
                .to_string(),
            format!("NamespaceNotFound => No such namespace: {non_existent_namespace_ident:?}")
        )
    }

    #[tokio::test]
    async fn test_dropping_a_namespace_also_drops_namespaces_nested_under_that_one() {
        let catalog = new_memory_catalog().await;
        let namespace_ident_a = NamespaceIdent::new("a".into());
        let namespace_ident_a_b = NamespaceIdent::from_strs(vec!["a", "b"]).unwrap();
        create_namespaces(&catalog, &vec![&namespace_ident_a, &namespace_ident_a_b]).await;

        catalog.drop_namespace(&namespace_ident_a).await.unwrap();

        assert!(!catalog.namespace_exists(&namespace_ident_a).await.unwrap());

        assert!(
            !catalog
                .namespace_exists(&namespace_ident_a_b)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn test_create_table_with_location() {
        let tmp_dir = TempDir::new().unwrap();
        let catalog = new_memory_catalog().await;
        let namespace_ident = NamespaceIdent::new("a".into());
        create_namespace(&catalog, &namespace_ident).await;

        let table_name = "abc";
        let location = tmp_dir.path().to_str().unwrap().to_string();
        let table_creation = TableCreation::builder()
            .name(table_name.into())
            .location(location.clone())
            .schema(simple_table_schema())
            .build();

        let expected_table_ident = TableIdent::new(namespace_ident.clone(), table_name.into());

        assert_table_eq(
            &catalog
                .create_table(&namespace_ident, table_creation)
                .await
                .unwrap(),
            &expected_table_ident,
            &simple_table_schema(),
        );

        let table = catalog.load_table(&expected_table_ident).await.unwrap();

        assert_table_eq(&table, &expected_table_ident, &simple_table_schema());

        assert!(
            table
                .metadata_location()
                .unwrap()
                .to_string()
                .starts_with(&location)
        )
    }

    #[tokio::test]
    async fn test_create_table_falls_back_to_namespace_location_if_table_location_is_missing() {
        let warehouse_location = temp_path();
        let catalog = MemoryCatalogBuilder::default()
            .load(
                "memory",
                HashMap::from([(
                    MEMORY_CATALOG_WAREHOUSE.to_string(),
                    warehouse_location.clone(),
                )]),
            )
            .await
            .unwrap();

        let namespace_ident = NamespaceIdent::new("a".into());
        let mut namespace_properties = HashMap::new();
        let namespace_location = temp_path();
        namespace_properties.insert(LOCATION.to_string(), namespace_location.to_string());
        catalog
            .create_namespace(&namespace_ident, namespace_properties)
            .await
            .unwrap();

        let table_name = "tbl1";
        let expected_table_ident = TableIdent::new(namespace_ident.clone(), table_name.into());
        let expected_table_metadata_location_regex =
            format!("^{namespace_location}/tbl1/metadata/00000-{UUID_REGEX_STR}.metadata.json$",);

        let table = catalog
            .create_table(
                &namespace_ident,
                TableCreation::builder()
                    .name(table_name.into())
                    .schema(simple_table_schema())
                    // no location specified for table
                    .build(),
            )
            .await
            .unwrap();
        assert_table_eq(&table, &expected_table_ident, &simple_table_schema());
        assert_table_metadata_location_matches(&table, &expected_table_metadata_location_regex);

        let table = catalog.load_table(&expected_table_ident).await.unwrap();
        assert_table_eq(&table, &expected_table_ident, &simple_table_schema());
        assert_table_metadata_location_matches(&table, &expected_table_metadata_location_regex);
    }

    #[tokio::test]
    async fn test_create_table_in_nested_namespace_falls_back_to_nested_namespace_location_if_table_location_is_missing()
     {
        let warehouse_location = temp_path();
        let catalog = MemoryCatalogBuilder::default()
            .load(
                "memory",
                HashMap::from([(
                    MEMORY_CATALOG_WAREHOUSE.to_string(),
                    warehouse_location.clone(),
                )]),
            )
            .await
            .unwrap();

        let namespace_ident = NamespaceIdent::new("a".into());
        let mut namespace_properties = HashMap::new();
        let namespace_location = temp_path();
        namespace_properties.insert(LOCATION.to_string(), namespace_location.to_string());
        catalog
            .create_namespace(&namespace_ident, namespace_properties)
            .await
            .unwrap();

        let nested_namespace_ident = NamespaceIdent::from_strs(vec!["a", "b"]).unwrap();
        let mut nested_namespace_properties = HashMap::new();
        let nested_namespace_location = temp_path();
        nested_namespace_properties
            .insert(LOCATION.to_string(), nested_namespace_location.to_string());
        catalog
            .create_namespace(&nested_namespace_ident, nested_namespace_properties)
            .await
            .unwrap();

        let table_name = "tbl1";
        let expected_table_ident =
            TableIdent::new(nested_namespace_ident.clone(), table_name.into());
        let expected_table_metadata_location_regex = format!(
            "^{nested_namespace_location}/tbl1/metadata/00000-{UUID_REGEX_STR}.metadata.json$",
        );

        let table = catalog
            .create_table(
                &nested_namespace_ident,
                TableCreation::builder()
                    .name(table_name.into())
                    .schema(simple_table_schema())
                    // no location specified for table
                    .build(),
            )
            .await
            .unwrap();
        assert_table_eq(&table, &expected_table_ident, &simple_table_schema());
        assert_table_metadata_location_matches(&table, &expected_table_metadata_location_regex);

        let table = catalog.load_table(&expected_table_ident).await.unwrap();
        assert_table_eq(&table, &expected_table_ident, &simple_table_schema());
        assert_table_metadata_location_matches(&table, &expected_table_metadata_location_regex);
    }

    #[tokio::test]
    async fn test_create_table_falls_back_to_warehouse_location_if_both_table_location_and_namespace_location_are_missing()
     {
        let warehouse_location = temp_path();
        let catalog = MemoryCatalogBuilder::default()
            .load(
                "memory",
                HashMap::from([(
                    MEMORY_CATALOG_WAREHOUSE.to_string(),
                    warehouse_location.clone(),
                )]),
            )
            .await
            .unwrap();

        let namespace_ident = NamespaceIdent::new("a".into());
        // note: no location specified in namespace_properties
        let namespace_properties = HashMap::new();
        catalog
            .create_namespace(&namespace_ident, namespace_properties)
            .await
            .unwrap();

        let table_name = "tbl1";
        let expected_table_ident = TableIdent::new(namespace_ident.clone(), table_name.into());
        let expected_table_metadata_location_regex =
            format!("^{warehouse_location}/a/tbl1/metadata/00000-{UUID_REGEX_STR}.metadata.json$");

        let table = catalog
            .create_table(
                &namespace_ident,
                TableCreation::builder()
                    .name(table_name.into())
                    .schema(simple_table_schema())
                    // no location specified for table
                    .build(),
            )
            .await
            .unwrap();
        assert_table_eq(&table, &expected_table_ident, &simple_table_schema());
        assert_table_metadata_location_matches(&table, &expected_table_metadata_location_regex);

        let table = catalog.load_table(&expected_table_ident).await.unwrap();
        assert_table_eq(&table, &expected_table_ident, &simple_table_schema());
        assert_table_metadata_location_matches(&table, &expected_table_metadata_location_regex);
    }

    #[tokio::test]
    async fn test_create_table_in_nested_namespace_falls_back_to_warehouse_location_if_both_table_location_and_namespace_location_are_missing()
     {
        let warehouse_location = temp_path();
        let catalog = MemoryCatalogBuilder::default()
            .load(
                "memory",
                HashMap::from([(
                    MEMORY_CATALOG_WAREHOUSE.to_string(),
                    warehouse_location.clone(),
                )]),
            )
            .await
            .unwrap();

        let namespace_ident = NamespaceIdent::new("a".into());
        catalog
            // note: no location specified in namespace_properties
            .create_namespace(&namespace_ident, HashMap::new())
            .await
            .unwrap();

        let nested_namespace_ident = NamespaceIdent::from_strs(vec!["a", "b"]).unwrap();
        catalog
            // note: no location specified in namespace_properties
            .create_namespace(&nested_namespace_ident, HashMap::new())
            .await
            .unwrap();

        let table_name = "tbl1";
        let expected_table_ident =
            TableIdent::new(nested_namespace_ident.clone(), table_name.into());
        let expected_table_metadata_location_regex = format!(
            "^{warehouse_location}/a/b/tbl1/metadata/00000-{UUID_REGEX_STR}.metadata.json$"
        );

        let table = catalog
            .create_table(
                &nested_namespace_ident,
                TableCreation::builder()
                    .name(table_name.into())
                    .schema(simple_table_schema())
                    // no location specified for table
                    .build(),
            )
            .await
            .unwrap();
        assert_table_eq(&table, &expected_table_ident, &simple_table_schema());
        assert_table_metadata_location_matches(&table, &expected_table_metadata_location_regex);

        let table = catalog.load_table(&expected_table_ident).await.unwrap();
        assert_table_eq(&table, &expected_table_ident, &simple_table_schema());
        assert_table_metadata_location_matches(&table, &expected_table_metadata_location_regex);
    }

    #[tokio::test]
    async fn test_create_table_throws_error_if_table_location_and_namespace_location_and_warehouse_location_are_missing()
     {
        let catalog = MemoryCatalogBuilder::default()
            .load("memory", HashMap::from([]))
            .await;

        assert!(catalog.is_err());
        assert_eq!(
            catalog.unwrap_err().to_string(),
            "DataInvalid => Catalog warehouse is required"
        );
    }

    #[tokio::test]
    async fn test_create_table_throws_error_if_table_with_same_name_already_exists() {
        let catalog = new_memory_catalog().await;
        let namespace_ident = NamespaceIdent::new("a".into());
        create_namespace(&catalog, &namespace_ident).await;
        let table_name = "tbl1";
        let table_ident = TableIdent::new(namespace_ident.clone(), table_name.into());
        create_table(&catalog, &table_ident).await;

        let tmp_dir = TempDir::new().unwrap();
        let location = tmp_dir.path().to_str().unwrap().to_string();

        assert_eq!(
            catalog
                .create_table(
                    &namespace_ident,
                    TableCreation::builder()
                        .name(table_name.into())
                        .schema(simple_table_schema())
                        .location(location)
                        .build()
                )
                .await
                .unwrap_err()
                .to_string(),
            format!(
                "TableAlreadyExists => Cannot create table {:?}. Table already exists.",
                &table_ident
            )
        );
    }

    #[tokio::test]
    async fn test_list_tables_returns_empty_vector() {
        let catalog = new_memory_catalog().await;
        let namespace_ident = NamespaceIdent::new("a".into());
        create_namespace(&catalog, &namespace_ident).await;

        assert_eq!(catalog.list_tables(&namespace_ident).await.unwrap(), vec![]);
    }

    #[tokio::test]
    async fn test_list_tables_returns_a_single_table() {
        let catalog = new_memory_catalog().await;
        let namespace_ident = NamespaceIdent::new("n1".into());
        create_namespace(&catalog, &namespace_ident).await;

        let table_ident = TableIdent::new(namespace_ident.clone(), "tbl1".into());
        create_table(&catalog, &table_ident).await;

        assert_eq!(catalog.list_tables(&namespace_ident).await.unwrap(), vec![
            table_ident
        ]);
    }

    #[tokio::test]
    async fn test_list_tables_returns_multiple_tables() {
        let catalog = new_memory_catalog().await;
        let namespace_ident = NamespaceIdent::new("n1".into());
        create_namespace(&catalog, &namespace_ident).await;

        let table_ident_1 = TableIdent::new(namespace_ident.clone(), "tbl1".into());
        let table_ident_2 = TableIdent::new(namespace_ident.clone(), "tbl2".into());
        let _ = create_tables(&catalog, vec![&table_ident_1, &table_ident_2]).await;

        assert_eq!(
            to_set(catalog.list_tables(&namespace_ident).await.unwrap()),
            to_set(vec![table_ident_1, table_ident_2])
        );
    }

    #[tokio::test]
    async fn test_list_tables_returns_tables_from_correct_namespace() {
        let catalog = new_memory_catalog().await;
        let namespace_ident_1 = NamespaceIdent::new("n1".into());
        let namespace_ident_2 = NamespaceIdent::new("n2".into());
        create_namespaces(&catalog, &vec![&namespace_ident_1, &namespace_ident_2]).await;

        let table_ident_1 = TableIdent::new(namespace_ident_1.clone(), "tbl1".into());
        let table_ident_2 = TableIdent::new(namespace_ident_1.clone(), "tbl2".into());
        let table_ident_3 = TableIdent::new(namespace_ident_2.clone(), "tbl1".into());
        let _ = create_tables(&catalog, vec![
            &table_ident_1,
            &table_ident_2,
            &table_ident_3,
        ])
        .await;

        assert_eq!(
            to_set(catalog.list_tables(&namespace_ident_1).await.unwrap()),
            to_set(vec![table_ident_1, table_ident_2])
        );

        assert_eq!(
            to_set(catalog.list_tables(&namespace_ident_2).await.unwrap()),
            to_set(vec![table_ident_3])
        );
    }

    #[tokio::test]
    async fn test_list_tables_returns_table_under_nested_namespace() {
        let catalog = new_memory_catalog().await;
        let namespace_ident_a = NamespaceIdent::new("a".into());
        let namespace_ident_a_b = NamespaceIdent::from_strs(vec!["a", "b"]).unwrap();
        create_namespaces(&catalog, &vec![&namespace_ident_a, &namespace_ident_a_b]).await;

        let table_ident = TableIdent::new(namespace_ident_a_b.clone(), "tbl1".into());
        create_table(&catalog, &table_ident).await;

        assert_eq!(
            catalog.list_tables(&namespace_ident_a_b).await.unwrap(),
            vec![table_ident]
        );
    }

    #[tokio::test]
    async fn test_list_tables_throws_error_if_namespace_doesnt_exist() {
        let catalog = new_memory_catalog().await;

        let non_existent_namespace_ident = NamespaceIdent::new("n1".into());

        assert_eq!(
            catalog
                .list_tables(&non_existent_namespace_ident)
                .await
                .unwrap_err()
                .to_string(),
            format!("NamespaceNotFound => No such namespace: {non_existent_namespace_ident:?}"),
        );
    }

    #[tokio::test]
    async fn test_drop_table() {
        let catalog = new_memory_catalog().await;
        let namespace_ident = NamespaceIdent::new("n1".into());
        create_namespace(&catalog, &namespace_ident).await;
        let table_ident = TableIdent::new(namespace_ident.clone(), "tbl1".into());
        create_table(&catalog, &table_ident).await;

        catalog.drop_table(&table_ident).await.unwrap();
    }

    #[tokio::test]
    async fn test_drop_table_drops_table_under_nested_namespace() {
        let catalog = new_memory_catalog().await;
        let namespace_ident_a = NamespaceIdent::new("a".into());
        let namespace_ident_a_b = NamespaceIdent::from_strs(vec!["a", "b"]).unwrap();
        create_namespaces(&catalog, &vec![&namespace_ident_a, &namespace_ident_a_b]).await;

        let table_ident = TableIdent::new(namespace_ident_a_b.clone(), "tbl1".into());
        create_table(&catalog, &table_ident).await;

        catalog.drop_table(&table_ident).await.unwrap();

        assert_eq!(
            catalog.list_tables(&namespace_ident_a_b).await.unwrap(),
            vec![]
        );
    }

    #[tokio::test]
    async fn test_drop_table_throws_error_if_namespace_doesnt_exist() {
        let catalog = new_memory_catalog().await;

        let non_existent_namespace_ident = NamespaceIdent::new("n1".into());
        let non_existent_table_ident =
            TableIdent::new(non_existent_namespace_ident.clone(), "tbl1".into());

        assert_eq!(
            catalog
                .drop_table(&non_existent_table_ident)
                .await
                .unwrap_err()
                .to_string(),
            format!("NamespaceNotFound => No such namespace: {non_existent_namespace_ident:?}"),
        );
    }

    #[tokio::test]
    async fn test_drop_table_throws_error_if_table_doesnt_exist() {
        let catalog = new_memory_catalog().await;
        let namespace_ident = NamespaceIdent::new("n1".into());
        create_namespace(&catalog, &namespace_ident).await;

        let non_existent_table_ident = TableIdent::new(namespace_ident.clone(), "tbl1".into());

        assert_eq!(
            catalog
                .drop_table(&non_existent_table_ident)
                .await
                .unwrap_err()
                .to_string(),
            format!("TableNotFound => No such table: {non_existent_table_ident:?}"),
        );
    }

    #[tokio::test]
    async fn test_table_exists_returns_true() {
        let catalog = new_memory_catalog().await;
        let namespace_ident = NamespaceIdent::new("n1".into());
        create_namespace(&catalog, &namespace_ident).await;
        let table_ident = TableIdent::new(namespace_ident.clone(), "tbl1".into());
        create_table(&catalog, &table_ident).await;

        assert!(catalog.table_exists(&table_ident).await.unwrap());
    }

    #[tokio::test]
    async fn test_table_exists_returns_false() {
        let catalog = new_memory_catalog().await;
        let namespace_ident = NamespaceIdent::new("n1".into());
        create_namespace(&catalog, &namespace_ident).await;
        let non_existent_table_ident = TableIdent::new(namespace_ident.clone(), "tbl1".into());

        assert!(
            !catalog
                .table_exists(&non_existent_table_ident)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn test_table_exists_under_nested_namespace() {
        let catalog = new_memory_catalog().await;
        let namespace_ident_a = NamespaceIdent::new("a".into());
        let namespace_ident_a_b = NamespaceIdent::from_strs(vec!["a", "b"]).unwrap();
        create_namespaces(&catalog, &vec![&namespace_ident_a, &namespace_ident_a_b]).await;

        let table_ident = TableIdent::new(namespace_ident_a_b.clone(), "tbl1".into());
        create_table(&catalog, &table_ident).await;

        assert!(catalog.table_exists(&table_ident).await.unwrap());

        let non_existent_table_ident = TableIdent::new(namespace_ident_a_b.clone(), "tbl2".into());
        assert!(
            !catalog
                .table_exists(&non_existent_table_ident)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn test_table_exists_throws_error_if_namespace_doesnt_exist() {
        let catalog = new_memory_catalog().await;

        let non_existent_namespace_ident = NamespaceIdent::new("n1".into());
        let non_existent_table_ident =
            TableIdent::new(non_existent_namespace_ident.clone(), "tbl1".into());

        assert_eq!(
            catalog
                .table_exists(&non_existent_table_ident)
                .await
                .unwrap_err()
                .to_string(),
            format!("NamespaceNotFound => No such namespace: {non_existent_namespace_ident:?}"),
        );
    }

    #[tokio::test]
    async fn test_rename_table_in_same_namespace() {
        let catalog = new_memory_catalog().await;
        let namespace_ident = NamespaceIdent::new("n1".into());
        create_namespace(&catalog, &namespace_ident).await;
        let src_table_ident = TableIdent::new(namespace_ident.clone(), "tbl1".into());
        let dst_table_ident = TableIdent::new(namespace_ident.clone(), "tbl2".into());
        create_table(&catalog, &src_table_ident).await;

        catalog
            .rename_table(&src_table_ident, &dst_table_ident)
            .await
            .unwrap();

        assert_eq!(catalog.list_tables(&namespace_ident).await.unwrap(), vec![
            dst_table_ident
        ],);
    }

    #[tokio::test]
    async fn test_rename_table_across_namespaces() {
        let catalog = new_memory_catalog().await;
        let src_namespace_ident = NamespaceIdent::new("a".into());
        let dst_namespace_ident = NamespaceIdent::new("b".into());
        create_namespaces(&catalog, &vec![&src_namespace_ident, &dst_namespace_ident]).await;
        let src_table_ident = TableIdent::new(src_namespace_ident.clone(), "tbl1".into());
        let dst_table_ident = TableIdent::new(dst_namespace_ident.clone(), "tbl2".into());
        create_table(&catalog, &src_table_ident).await;

        catalog
            .rename_table(&src_table_ident, &dst_table_ident)
            .await
            .unwrap();

        assert_eq!(
            catalog.list_tables(&src_namespace_ident).await.unwrap(),
            vec![],
        );

        assert_eq!(
            catalog.list_tables(&dst_namespace_ident).await.unwrap(),
            vec![dst_table_ident],
        );
    }

    #[tokio::test]
    async fn test_rename_table_src_table_is_same_as_dst_table() {
        let catalog = new_memory_catalog().await;
        let namespace_ident = NamespaceIdent::new("n1".into());
        create_namespace(&catalog, &namespace_ident).await;
        let table_ident = TableIdent::new(namespace_ident.clone(), "tbl".into());
        create_table(&catalog, &table_ident).await;

        catalog
            .rename_table(&table_ident, &table_ident)
            .await
            .unwrap();

        assert_eq!(catalog.list_tables(&namespace_ident).await.unwrap(), vec![
            table_ident
        ],);
    }

    #[tokio::test]
    async fn test_rename_table_across_nested_namespaces() {
        let catalog = new_memory_catalog().await;
        let namespace_ident_a = NamespaceIdent::new("a".into());
        let namespace_ident_a_b = NamespaceIdent::from_strs(vec!["a", "b"]).unwrap();
        let namespace_ident_a_b_c = NamespaceIdent::from_strs(vec!["a", "b", "c"]).unwrap();
        create_namespaces(&catalog, &vec![
            &namespace_ident_a,
            &namespace_ident_a_b,
            &namespace_ident_a_b_c,
        ])
        .await;

        let src_table_ident = TableIdent::new(namespace_ident_a_b_c.clone(), "tbl1".into());
        create_tables(&catalog, vec![&src_table_ident]).await;

        let dst_table_ident = TableIdent::new(namespace_ident_a_b.clone(), "tbl1".into());
        catalog
            .rename_table(&src_table_ident, &dst_table_ident)
            .await
            .unwrap();

        assert!(!catalog.table_exists(&src_table_ident).await.unwrap());

        assert!(catalog.table_exists(&dst_table_ident).await.unwrap());
    }

    #[tokio::test]
    async fn test_rename_table_throws_error_if_src_namespace_doesnt_exist() {
        let catalog = new_memory_catalog().await;

        let non_existent_src_namespace_ident = NamespaceIdent::new("n1".into());
        let src_table_ident =
            TableIdent::new(non_existent_src_namespace_ident.clone(), "tbl1".into());

        let dst_namespace_ident = NamespaceIdent::new("n2".into());
        create_namespace(&catalog, &dst_namespace_ident).await;
        let dst_table_ident = TableIdent::new(dst_namespace_ident.clone(), "tbl1".into());

        assert_eq!(
            catalog
                .rename_table(&src_table_ident, &dst_table_ident)
                .await
                .unwrap_err()
                .to_string(),
            format!("NamespaceNotFound => No such namespace: {non_existent_src_namespace_ident:?}"),
        );
    }

    #[tokio::test]
    async fn test_rename_table_throws_error_if_dst_namespace_doesnt_exist() {
        let catalog = new_memory_catalog().await;
        let src_namespace_ident = NamespaceIdent::new("n1".into());
        let src_table_ident = TableIdent::new(src_namespace_ident.clone(), "tbl1".into());
        create_namespace(&catalog, &src_namespace_ident).await;
        create_table(&catalog, &src_table_ident).await;

        let non_existent_dst_namespace_ident = NamespaceIdent::new("n2".into());
        let dst_table_ident =
            TableIdent::new(non_existent_dst_namespace_ident.clone(), "tbl1".into());
        assert_eq!(
            catalog
                .rename_table(&src_table_ident, &dst_table_ident)
                .await
                .unwrap_err()
                .to_string(),
            format!("NamespaceNotFound => No such namespace: {non_existent_dst_namespace_ident:?}"),
        );
    }

    #[tokio::test]
    async fn test_rename_table_throws_error_if_src_table_doesnt_exist() {
        let catalog = new_memory_catalog().await;
        let namespace_ident = NamespaceIdent::new("n1".into());
        create_namespace(&catalog, &namespace_ident).await;
        let src_table_ident = TableIdent::new(namespace_ident.clone(), "tbl1".into());
        let dst_table_ident = TableIdent::new(namespace_ident.clone(), "tbl2".into());

        assert_eq!(
            catalog
                .rename_table(&src_table_ident, &dst_table_ident)
                .await
                .unwrap_err()
                .to_string(),
            format!("TableNotFound => No such table: {src_table_ident:?}"),
        );
    }

    #[tokio::test]
    async fn test_rename_table_throws_error_if_dst_table_already_exists() {
        let catalog = new_memory_catalog().await;
        let namespace_ident = NamespaceIdent::new("n1".into());
        create_namespace(&catalog, &namespace_ident).await;
        let src_table_ident = TableIdent::new(namespace_ident.clone(), "tbl1".into());
        let dst_table_ident = TableIdent::new(namespace_ident.clone(), "tbl2".into());
        create_tables(&catalog, vec![&src_table_ident, &dst_table_ident]).await;

        assert_eq!(
            catalog
                .rename_table(&src_table_ident, &dst_table_ident)
                .await
                .unwrap_err()
                .to_string(),
            format!(
                "TableAlreadyExists => Cannot create table {:?}. Table already exists.",
                &dst_table_ident
            ),
        );
    }

    #[tokio::test]
    async fn test_register_table() {
        // Create a catalog and namespace
        let catalog = new_memory_catalog().await;
        let namespace_ident = NamespaceIdent::new("test_namespace".into());
        create_namespace(&catalog, &namespace_ident).await;

        // Create a table to get a valid metadata file
        let source_table_ident = TableIdent::new(namespace_ident.clone(), "source_table".into());
        create_table(&catalog, &source_table_ident).await;

        // Get the metadata location from the source table
        let source_table = catalog.load_table(&source_table_ident).await.unwrap();
        let metadata_location = source_table.metadata_location().unwrap().to_string();

        // Register a new table using the same metadata location
        let register_table_ident =
            TableIdent::new(namespace_ident.clone(), "register_table".into());
        let registered_table = catalog
            .register_table(&register_table_ident, metadata_location.clone())
            .await
            .unwrap();

        // Verify the registered table has the correct identifier
        assert_eq!(registered_table.identifier(), &register_table_ident);

        // Verify the registered table has the correct metadata location
        assert_eq!(
            registered_table.metadata_location().unwrap().to_string(),
            metadata_location
        );

        // Verify the table exists in the catalog
        assert!(catalog.table_exists(&register_table_ident).await.unwrap());

        // Verify we can load the registered table
        let loaded_table = catalog.load_table(&register_table_ident).await.unwrap();
        assert_eq!(loaded_table.identifier(), &register_table_ident);
        assert_eq!(
            loaded_table.metadata_location().unwrap().to_string(),
            metadata_location
        );
    }

    #[tokio::test]
    async fn test_update_table() {
        let catalog = new_memory_catalog().await;

        let table = create_table_with_namespace(&catalog).await;

        // Assert the table doesn't contain the update yet
        assert!(!table.metadata().properties().contains_key("key"));

        // Update table metadata
        let tx = Transaction::new(&table);
        let updated_table = tx
            .update_table_properties()
            .set("key".to_string(), "value".to_string())
            .apply(tx)
            .unwrap()
            .commit(&catalog)
            .await
            .unwrap();

        assert_eq!(
            updated_table.metadata().properties().get("key").unwrap(),
            "value"
        );

        assert_eq!(table.identifier(), updated_table.identifier());
        assert_eq!(table.metadata().uuid(), updated_table.metadata().uuid());
        assert!(table.metadata().last_updated_ms() < updated_table.metadata().last_updated_ms());
        assert_ne!(table.metadata_location(), updated_table.metadata_location());

        assert!(
            table.metadata().metadata_log().len() < updated_table.metadata().metadata_log().len()
        );
    }

    #[tokio::test]
    async fn test_update_table_fails_if_table_doesnt_exist() {
        let catalog = new_memory_catalog().await;

        let namespace_ident = NamespaceIdent::new("a".into());
        create_namespace(&catalog, &namespace_ident).await;

        // This table is not known to the catalog.
        let table_ident = TableIdent::new(namespace_ident, "test".to_string());
        let table = build_table(table_ident);

        let tx = Transaction::new(&table);
        let err = tx
            .update_table_properties()
            .set("key".to_string(), "value".to_string())
            .apply(tx)
            .unwrap()
            .commit(&catalog)
            .await
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::TableNotFound);
    }

    fn build_table(ident: TableIdent) -> Table {
        let file_io = FileIOBuilder::new_fs_io().build().unwrap();

        let temp_dir = TempDir::new().unwrap();
        let location = temp_dir.path().to_str().unwrap().to_string();

        let table_creation = TableCreation::builder()
            .name(ident.name().to_string())
            .schema(simple_table_schema())
            .location(location)
            .build();
        let metadata = TableMetadataBuilder::from_table_creation(table_creation)
            .unwrap()
            .build()
            .unwrap()
            .metadata;

        Table::builder()
            .identifier(ident)
            .metadata(metadata)
            .file_io(file_io)
            .build()
            .unwrap()
    }
}
