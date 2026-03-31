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

mod datafusion;

use std::collections::HashMap;
use std::path::Path;

use anyhow::anyhow;
use serde::Deserialize;
use sqllogictest::{AsyncDB, MakeConnection, Runner, parse_file};

use crate::engine::datafusion::DataFusionEngine;
use crate::error::{Error, Result};

/// Configuration for the catalog used by the DataFusion engine
#[derive(Debug, Clone, Deserialize)]
pub struct DatafusionCatalogConfig {
    /// Catalog type: "memory", "rest", "glue", "hms", "s3tables", "sql"
    #[serde(rename = "type")]
    pub catalog_type: String,
    /// Catalog properties passed to the catalog loader
    #[serde(default)]
    pub props: HashMap<String, String>,
}

/// Engine configuration as a tagged enum
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum EngineConfig {
    Datafusion {
        #[serde(default)]
        catalog: Option<DatafusionCatalogConfig>,
    },
}

#[async_trait::async_trait]
pub trait EngineRunner: Send {
    async fn run_slt_file(&mut self, path: &Path) -> Result<()>;
}

pub async fn load_engine_runner(config: EngineConfig) -> Result<Box<dyn EngineRunner>> {
    match config {
        EngineConfig::Datafusion { catalog } => Ok(Box::new(DataFusionEngine::new(catalog).await?)),
    }
}

pub async fn run_slt_with_runner<D, M>(
    mut runner: Runner<D, M>,
    step_slt_file: impl AsRef<Path>,
) -> Result<()>
where
    D: AsyncDB + Send + 'static,
    M: MakeConnection<Conn = D> + Send + 'static,
{
    let path = step_slt_file.as_ref().canonicalize()?;
    let records = parse_file(&path).map_err(|e| Error(anyhow!("parsing slt file failed: {e}")))?;

    for record in records {
        if let Err(err) = runner.run_async(record).await {
            return Err(Error(anyhow!("SLT record execution failed: {err}")));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::engine::{DatafusionCatalogConfig, EngineConfig, load_engine_runner};

    #[test]
    fn test_deserialize_engine_config() {
        let input = r#"type = "datafusion""#;

        let config: EngineConfig = toml::from_str(input).unwrap();
        assert!(matches!(config, EngineConfig::Datafusion { catalog: None }));
    }

    #[test]
    fn test_deserialize_engine_config_with_catalog() {
        let input = r#"
            type = "datafusion"

            [catalog]
            type = "rest"

            [catalog.props]
            uri = "http://localhost:8181"
        "#;

        let config: EngineConfig = toml::from_str(input).unwrap();
        match config {
            EngineConfig::Datafusion { catalog: Some(cat) } => {
                assert_eq!(cat.catalog_type, "rest");
                assert_eq!(
                    cat.props.get("uri"),
                    Some(&"http://localhost:8181".to_string())
                );
            }
            _ => panic!("Expected Datafusion with catalog"),
        }
    }

    #[test]
    fn test_deserialize_catalog_config() {
        let input = r#"
            type = "memory"

            [props]
            warehouse = "file:///tmp/warehouse"
        "#;

        let config: DatafusionCatalogConfig = toml::from_str(input).unwrap();
        assert_eq!(config.catalog_type, "memory");
        assert_eq!(
            config.props.get("warehouse"),
            Some(&"file:///tmp/warehouse".to_string())
        );
    }

    #[tokio::test]
    async fn test_load_datafusion() {
        let config = EngineConfig::Datafusion { catalog: None };

        let result = load_engine_runner(config).await;
        assert!(result.is_ok());
    }
}
