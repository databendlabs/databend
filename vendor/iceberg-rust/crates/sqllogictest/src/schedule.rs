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
use std::fs::read_to_string;
use std::path::{Path, PathBuf};

use anyhow::{Context, anyhow};
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::engine::{EngineConfig, EngineRunner, load_engine_runner};

/// Raw configuration parsed from the schedule TOML file
#[derive(Debug, Clone, Deserialize)]
pub struct ScheduleConfig {
    /// Engine name to engine configuration
    pub engines: HashMap<String, EngineConfig>,
    /// List of test steps to run
    pub steps: Vec<Step>,
}

pub struct Schedule {
    /// Engine names to engine instances
    engines: HashMap<String, Box<dyn EngineRunner>>,
    /// List of test steps to run
    steps: Vec<Step>,
    /// Path of the schedule file
    schedule_file: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Step {
    /// Engine name
    engine: String,
    /// Stl file path
    slt: String,
}

impl Schedule {
    pub fn new(
        engines: HashMap<String, Box<dyn EngineRunner>>,
        steps: Vec<Step>,
        schedule_file: String,
    ) -> Self {
        Self {
            engines,
            steps,
            schedule_file,
        }
    }

    pub async fn from_file<P: AsRef<Path>>(path: P) -> anyhow::Result<Self> {
        let path_str = path.as_ref().to_string_lossy().to_string();
        let content = read_to_string(path)?;

        let config: ScheduleConfig = toml::from_str(&content)
            .with_context(|| format!("Failed to parse schedule file: {path_str}"))?;

        let engines = Self::instantiate_engines(config.engines).await?;

        Ok(Self::new(engines, config.steps, path_str))
    }

    /// Instantiate engine runners from their configurations
    async fn instantiate_engines(
        configs: HashMap<String, EngineConfig>,
    ) -> anyhow::Result<HashMap<String, Box<dyn EngineRunner>>> {
        let mut engines = HashMap::new();

        for (name, config) in configs {
            let engine = load_engine_runner(config).await?;
            engines.insert(name, engine);
        }

        Ok(engines)
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        info!("Starting test run with schedule: {}", self.schedule_file);

        for (idx, step) in self.steps.iter().enumerate() {
            info!(
                "Running step {}/{}, using engine {}, slt file path: {}",
                idx + 1,
                self.steps.len(),
                &step.engine,
                &step.slt
            );

            let engine = self
                .engines
                .get_mut(&step.engine)
                .ok_or_else(|| anyhow!("Engine {} not found", step.engine))?;

            let step_sql_path = PathBuf::from(format!(
                "{}/testdata/slts/{}",
                env!("CARGO_MANIFEST_DIR"),
                &step.slt
            ));

            engine.run_slt_file(&step_sql_path).await?;

            info!(
                "Completed step {}/{}, engine {}, slt file path: {}",
                idx + 1,
                self.steps.len(),
                &step.engine,
                &step.slt
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::engine::EngineConfig;
    use crate::schedule::ScheduleConfig;

    #[test]
    fn test_deserialize_schedule_config() {
        let input = r#"
            [engines]
            df = { type = "datafusion" }

            [[steps]]
            engine = "df"
            slt = "test.slt"
        "#;

        let config: ScheduleConfig = toml::from_str(input).unwrap();

        assert_eq!(config.engines.len(), 1);
        assert!(config.engines.contains_key("df"));
        assert!(matches!(config.engines["df"], EngineConfig::Datafusion {
            catalog: None
        }));
        assert_eq!(config.steps.len(), 1);
        assert_eq!(config.steps[0].engine, "df");
        assert_eq!(config.steps[0].slt, "test.slt");
    }

    #[test]
    fn test_deserialize_multiple_steps() {
        let input = r#"
            [engines]
            datafusion = { type = "datafusion" }

            [[steps]]
            engine = "datafusion"
            slt = "test.slt"

            [[steps]]
            engine = "datafusion"
            slt = "test2.slt"
        "#;

        let config: ScheduleConfig = toml::from_str(input).unwrap();

        assert_eq!(config.steps.len(), 2);
        assert_eq!(config.steps[0].engine, "datafusion");
        assert_eq!(config.steps[0].slt, "test.slt");
        assert_eq!(config.steps[1].engine, "datafusion");
        assert_eq!(config.steps[1].slt, "test2.slt");
    }

    #[test]
    fn test_deserialize_with_catalog_config() {
        let input = r#"
            [engines.df]
            type = "datafusion"

            [engines.df.catalog]
            type = "rest"

            [engines.df.catalog.props]
            uri = "http://localhost:8181"

            [[steps]]
            engine = "df"
            slt = "test.slt"
        "#;

        let config: ScheduleConfig = toml::from_str(input).unwrap();

        match &config.engines["df"] {
            EngineConfig::Datafusion { catalog: Some(cat) } => {
                assert_eq!(cat.catalog_type, "rest");
                assert_eq!(
                    cat.props.get("uri"),
                    Some(&"http://localhost:8181".to_string())
                );
            }
            _ => panic!("Expected Datafusion with catalog config"),
        }
    }

    #[test]
    fn test_deserialize_missing_engine_type() {
        let input = r#"
            [engines]
            df = { }

            [[steps]]
            engine = "df"
            slt = "test.slt"
        "#;

        let result: Result<ScheduleConfig, _> = toml::from_str(input);
        assert!(result.is_err());
    }

    #[test]
    fn test_deserialize_invalid_engine_type() {
        let input = r#"
            [engines]
            df = { type = "unknown_engine" }

            [[steps]]
            engine = "df"
            slt = "test.slt"
        "#;

        let result: Result<ScheduleConfig, _> = toml::from_str(input);
        assert!(result.is_err());
    }

    #[test]
    fn test_deserialize_missing_step_fields() {
        let input = r#"
            [engines]
            df = { type = "datafusion" }

            [[steps]]
        "#;

        let result: Result<ScheduleConfig, _> = toml::from_str(input);
        assert!(result.is_err());
    }
}
