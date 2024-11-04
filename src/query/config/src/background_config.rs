// Copyright 2021 Datafuse Labs
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

use std::fmt::Debug;
use std::fmt::Formatter;
use std::str::FromStr;

use clap::Args;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::background::BackgroundJobParams;
use databend_common_meta_app::background::BackgroundJobType;
use serde::Deserialize;
use serde::Serialize;

#[derive(Clone, Debug, PartialEq, Default, Eq, Serialize, Deserialize, Args)]
#[serde(default)]
pub struct BackgroundConfig {
    #[clap(long = "enable-background-service", value_name = "VALUE")]
    pub enable: bool,
    // Fs compaction related background config.
    #[clap(flatten)]
    pub compaction: BackgroundCompactionConfig,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, Args)]
#[serde(default)]
pub struct BackgroundCompactionConfig {
    // only wake up background job if it is enabled.
    #[clap(long, value_name = "VALUE")]
    pub enable_compaction: bool,
    #[clap(long, value_name = "VALUE", default_value = "one_shot")]
    pub compact_mode: String,

    // Only compact tables in this list.
    // if it is empty, compact job would discover target tables automatically
    // otherwise, the job would only compact tables in this list
    #[clap(long, value_name = "VALUE")]
    pub target_tables: Option<Vec<String>>,

    // Compact segments if a table has too many small segments
    // `segment_limit` is the maximum number of segments that would be compacted in a batch
    // None represent their is no limit
    // Details: https://docs.databend.com/sql/sql-commands/ddl/table/optimize-table#segment-compaction
    #[clap(long, value_name = "VALUE")]
    pub segment_limit: Option<u64>,

    // Compact small blocks into large one.
    // `block_limit` is the maximum number of blocks that would be compacted in a batch
    // None represent their is no limit
    // Details: https://docs.databend.com/sql/sql-commands/ddl/table/optimize-table#segment-compaction
    #[clap(long, value_name = "VALUE")]
    pub block_limit: Option<u64>,

    #[clap(flatten)]
    pub scheduled_config: BackgroundScheduledConfig,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, Args)]
#[serde(default)]
pub struct BackgroundScheduledConfig {
    // the fixed interval for compaction on each table.
    #[clap(long, value_name = "VALUE", default_value = "1800")]
    pub duration_secs: u64,

    // the cron expression for scheduled job,
    // by default it is scheduled with UTC timezone
    #[clap(long, value_name = "VALUE", default_value = "")]
    pub cron: String,

    #[clap(long, value_name = "VALUE")]
    pub time_zone: Option<String>,
}

impl BackgroundScheduledConfig {
    pub fn new_interval_job(duration_secs: u64) -> Self {
        Self {
            duration_secs,
            cron: "".to_string(),
            time_zone: None,
        }
    }

    pub fn new_cron_job(cron: String, time_zone: Option<String>) -> Self {
        Self {
            duration_secs: 0,
            cron,
            time_zone,
        }
    }
}

/// Config for background config
#[derive(Clone, PartialEq, Eq)]
pub struct InnerBackgroundConfig {
    pub enable: bool,
    pub compaction: InnerBackgroundCompactionConfig,
}

#[derive(Clone, PartialEq, Eq)]
pub struct InnerBackgroundCompactionConfig {
    pub enable: bool,
    pub target_tables: Option<Vec<String>>,
    pub segment_limit: Option<u64>,
    pub block_limit: Option<u64>,
    pub params: BackgroundJobParams,
}

impl InnerBackgroundCompactionConfig {
    pub fn has_target_tables(&self) -> bool {
        self.target_tables.is_some() && !self.target_tables.as_ref().unwrap().is_empty()
    }
}

impl TryInto<InnerBackgroundConfig> for BackgroundConfig {
    type Error = ErrorCode;

    fn try_into(self) -> Result<InnerBackgroundConfig> {
        Ok(InnerBackgroundConfig {
            enable: self.enable,
            compaction: self.compaction.try_into()?,
        })
    }
}

impl From<InnerBackgroundConfig> for BackgroundConfig {
    fn from(inner: InnerBackgroundConfig) -> Self {
        Self {
            enable: inner.enable,
            compaction: BackgroundCompactionConfig::from(inner.compaction),
        }
    }
}

impl TryInto<InnerBackgroundCompactionConfig> for BackgroundCompactionConfig {
    type Error = ErrorCode;

    fn try_into(self) -> Result<InnerBackgroundCompactionConfig> {
        Ok(InnerBackgroundCompactionConfig {
            segment_limit: self.segment_limit,
            block_limit: self.block_limit,
            enable: self.enable_compaction,
            target_tables: self.target_tables,
            params: {
                match self.compact_mode.as_str() {
                    "one_shot" => BackgroundJobParams::new_one_shot_job(),
                    "interval" => BackgroundJobParams::new_interval_job(
                        std::time::Duration::from_secs(self.scheduled_config.duration_secs),
                    ),
                    "cron" => {
                        if self.scheduled_config.cron.is_empty() {
                            return Err(ErrorCode::InvalidArgument(
                                "cron expression is empty".to_string(),
                            ));
                        }
                        let tz = self
                            .scheduled_config
                            .time_zone
                            .clone()
                            .map(|x| chrono_tz::Tz::from_str(&x))
                            .transpose()
                            .map_err(|e| {
                                ErrorCode::InvalidArgument(format!("invalid time_zone: {}", e))
                            })?;
                        BackgroundJobParams::new_cron_job(self.scheduled_config.cron, tz)
                    }

                    _ => {
                        return Err(ErrorCode::InvalidArgument(format!(
                            "invalid compact_mode: {}",
                            self.compact_mode
                        )));
                    }
                }
            },
        })
    }
}

impl From<InnerBackgroundCompactionConfig> for BackgroundCompactionConfig {
    fn from(inner: InnerBackgroundCompactionConfig) -> Self {
        let mut cfg = Self {
            enable_compaction: inner.enable,
            compact_mode: "".to_string(), // it would be set later
            target_tables: inner.target_tables,
            segment_limit: inner.segment_limit,
            block_limit: inner.block_limit,
            scheduled_config: Default::default(), // it would be set later
        };
        match inner.params.job_type {
            BackgroundJobType::ONESHOT => {
                cfg.compact_mode = "one_shot".to_string();
            }
            BackgroundJobType::INTERVAL => {
                cfg.compact_mode = "interval".to_string();
                cfg.scheduled_config = BackgroundScheduledConfig::new_interval_job(
                    inner.params.scheduled_job_interval.as_secs(),
                );
            }
            BackgroundJobType::CRON => {
                cfg.compact_mode = "cron".to_string();
                cfg.scheduled_config = BackgroundScheduledConfig::new_cron_job(
                    inner.params.scheduled_job_cron,
                    inner.params.scheduled_job_timezone.map(|x| x.to_string()),
                );
            }
        }
        cfg
    }
}

impl From<BackgroundJobParams> for BackgroundScheduledConfig {
    fn from(inner: BackgroundJobParams) -> Self {
        Self {
            duration_secs: inner.scheduled_job_interval.as_secs(),
            cron: inner.scheduled_job_cron.clone(),
            time_zone: inner.scheduled_job_timezone.map(|x| x.to_string()),
        }
    }
}

impl Default for BackgroundCompactionConfig {
    fn default() -> Self {
        Self {
            enable_compaction: false,
            compact_mode: "one_shot".to_string(),
            target_tables: None,
            segment_limit: None,
            block_limit: None,
            scheduled_config: Default::default(),
        }
    }
}

impl Debug for BackgroundCompactionConfig {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        f.debug_struct("BackgroundCompactionConfig")
            .field("mode", &self.compact_mode)
            .field("segment_limit", &self.segment_limit)
            .field("block_limit", &self.block_limit)
            .field("fixed_config", &self.scheduled_config)
            .finish()
    }
}

impl Default for BackgroundScheduledConfig {
    fn default() -> Self {
        Self {
            duration_secs: 1800,
            cron: "".to_string(),
            time_zone: None,
        }
    }
}

impl Debug for BackgroundScheduledConfig {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        f.debug_struct("BackgroundCompactionFixedConfig")
            .field("duration_secs", &self.duration_secs)
            .finish()
    }
}

impl Default for InnerBackgroundConfig {
    fn default() -> Self {
        Self {
            enable: false,
            compaction: InnerBackgroundCompactionConfig {
                enable: false,
                target_tables: None,
                segment_limit: None,
                block_limit: None,
                params: Default::default(),
            },
        }
    }
}

impl Debug for InnerBackgroundConfig {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        f.debug_struct("InnerBackgroundConfig")
            .field("compaction", &self.compaction)
            .finish()
    }
}

impl Debug for InnerBackgroundCompactionConfig {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        f.debug_struct("InnerBackgroundCompactionConfig")
            .field("segment_limit", &self.segment_limit)
            .field("block_limit", &self.block_limit)
            .field("params", &self.params)
            .finish()
    }
}
