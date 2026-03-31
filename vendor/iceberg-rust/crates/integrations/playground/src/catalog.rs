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

use std::any::Any;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use anyhow::anyhow;
use datafusion::catalog::{CatalogProvider, CatalogProviderList};
use fs_err::read_to_string;
use iceberg::CatalogBuilder;
use iceberg_catalog_rest::RestCatalogBuilder;
use iceberg_datafusion::IcebergCatalogProvider;
use toml::{Table as TomlTable, Value};

const CONFIG_NAME_CATALOGS: &str = "catalogs";

#[derive(Debug)]
pub struct IcebergCatalogList {
    catalogs: HashMap<String, Arc<IcebergCatalogProvider>>,
}

impl IcebergCatalogList {
    pub async fn parse(path: &Path) -> anyhow::Result<Self> {
        let toml_table: TomlTable = toml::from_str(&read_to_string(path)?)?;
        Self::parse_table(&toml_table).await
    }

    pub async fn parse_table(configs: &TomlTable) -> anyhow::Result<Self> {
        if let Value::Array(catalogs_config) =
            configs.get(CONFIG_NAME_CATALOGS).ok_or_else(|| {
                anyhow::Error::msg(format!("{CONFIG_NAME_CATALOGS} entry not found in config"))
            })?
        {
            let mut catalogs = HashMap::with_capacity(catalogs_config.len());
            for config in catalogs_config {
                if let Value::Table(table_config) = config {
                    let (name, catalog_provider) =
                        IcebergCatalogList::parse_one(table_config).await?;
                    catalogs.insert(name, catalog_provider);
                } else {
                    return Err(anyhow!("{CONFIG_NAME_CATALOGS} entry must be a table"));
                }
            }
            Ok(Self { catalogs })
        } else {
            Err(anyhow!("{CONFIG_NAME_CATALOGS} must be an array of table!"))
        }
    }

    async fn parse_one(
        config: &TomlTable,
    ) -> anyhow::Result<(String, Arc<IcebergCatalogProvider>)> {
        let name = config
            .get("name")
            .ok_or_else(|| anyhow::anyhow!("name not found for catalog"))?
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("name is not string"))?;

        let r#type = config
            .get("type")
            .ok_or_else(|| anyhow::anyhow!("type not found for catalog"))?
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("type is not string"))?;

        if r#type != "rest" {
            return Err(anyhow::anyhow!("Only rest catalog is supported for now!"));
        }

        let catalog_config = config
            .get("config")
            .ok_or_else(|| anyhow::anyhow!("config not found for catalog {name}"))?
            .as_table()
            .ok_or_else(|| anyhow::anyhow!("config is not table for catalog {name}"))?;

        // parse all config into props
        let mut props = HashMap::with_capacity(catalog_config.len());
        for (key, value) in catalog_config {
            let value_str = value
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("props {key} is not string"))?;
            props.insert(key.to_string(), value_str.to_string());
        }
        let catalog = RestCatalogBuilder::default().load(name, props).await?;

        Ok((
            name.to_string(),
            Arc::new(IcebergCatalogProvider::try_new(Arc::new(catalog)).await?),
        ))
    }
}

impl CatalogProviderList for IcebergCatalogList {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn register_catalog(
        &self,
        _name: String,
        _catalog: Arc<dyn CatalogProvider>,
    ) -> Option<Arc<dyn CatalogProvider>> {
        tracing::error!("Registering catalog is not supported yet");
        None
    }

    fn catalog_names(&self) -> Vec<String> {
        self.catalogs.keys().cloned().collect()
    }

    fn catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>> {
        self.catalogs
            .get(name)
            .map(|c| c.clone() as Arc<dyn CatalogProvider>)
    }
}
