use std::fmt::Debug;
use std::fmt::Formatter;

use clap::Args;
use common_exception::ErrorCode;
use common_exception::Result;
use serde::Deserialize;
use serde::Serialize;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Args)]
#[serde(default)]
pub struct BackgroundConfig {
    #[clap(long = "enable-background-service")]
    pub enable: bool,
    // Fs compaction related background config.
    #[clap(flatten)]
    pub compaction: BackgroundCompactionConfig,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, Args)]
#[serde(default)]
pub struct BackgroundCompactionConfig {
    #[clap(long, default_value = "one_shot")]
    pub compact_mode: String,

    // Compact segments if a table has too many small segments
    // `segment_limit` is the maximum number of segments that would be compacted in a batch
    // None represent their is no limit
    // Details: https://databend.rs/doc/sql-commands/ddl/table/optimize-table#segment-compaction
    #[clap(long)]
    pub segment_limit: Option<u64>,

    // Compact small blocks into large one.
    // `block_limit` is the maximum number of blocks that would be compacted in a batch
    // None represent their is no limit
    // Details: https://databend.rs/doc/sql-commands/ddl/table/optimize-table#block-compaction
    #[clap(long)]
    pub block_limit: Option<u64>,

    #[clap(flatten)]
    pub fixed_config: BackgroundCompactionFixedConfig,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, Args)]
#[serde(default)]
pub struct BackgroundCompactionFixedConfig {
    // the fixed interval for compaction on each table.
    #[clap(long, default_value = "1800")]
    pub duration_secs: i64,
}

/// Config for background config
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InnerBackgroundConfig {
    pub enable: bool,
    pub compaction: InnerBackgroundCompactionConfig,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InnerBackgroundCompactionConfig {
    pub segment_limit: Option<u64>,
    pub block_limit: Option<u64>,
    pub params: CompactionParams,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode")]
pub enum CompactionParams {
    OneShot,
    Fixed(CompactionFixedConfig),
}

impl ToString for CompactionParams {
    fn to_string(&self) -> String {
        match self {
            CompactionParams::OneShot => "one_shot".to_string(),
            CompactionParams::Fixed(cfg) => format!("fixed: {:?}", cfg.duration_secs),
        }
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompactionFixedConfig {
    pub duration_secs: std::time::Duration,
}

impl Debug for CompactionFixedConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompactionFixedConfig")
            .field("duration_secs", &self.duration_secs)
            .finish()
    }
}
impl Default for CompactionFixedConfig {
    fn default() -> Self {
        Self {
            duration_secs: std::time::Duration::from_secs(1800),
        }
    }
}

impl Default for CompactionParams {
    fn default() -> Self {
        CompactionParams::Fixed(CompactionFixedConfig::default())
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
            params: {
                match self.compact_mode.as_str() {
                    "one_shot" => CompactionParams::OneShot,
                    "fixed" => CompactionParams::Fixed(self.fixed_config.try_into()?),
                    _ => return Err(ErrorCode::InvalidArgument(format!("invalid compact_mode: {}", self.compact_mode))),
                }
            },
        })
    }
}

impl From<InnerBackgroundCompactionConfig> for BackgroundCompactionConfig {
    fn from(inner: InnerBackgroundCompactionConfig) -> Self {
        let mut cfg = Self {
            compact_mode: "".to_string(),
            segment_limit: inner.segment_limit,
            block_limit: inner.block_limit,
            fixed_config: Default::default(),
        };
        match inner.params {
            CompactionParams::OneShot => {
                cfg.compact_mode = "one_shot".to_string();
            }
            CompactionParams::Fixed(v) => {
                cfg.compact_mode = "fixed".to_string();
                cfg.fixed_config = v.into();
            }
        }
        return cfg;
    }
}

impl From<CompactionFixedConfig> for BackgroundCompactionFixedConfig {
    fn from(inner: CompactionFixedConfig) -> Self {
        Self {
            duration_secs: inner.duration_secs.as_secs() as i64,
        }
    }
}

impl TryInto<CompactionFixedConfig> for BackgroundCompactionFixedConfig {
    type Error = ErrorCode;

    fn try_into(self) -> Result<CompactionFixedConfig> {
        Ok(CompactionFixedConfig {
            duration_secs: std::time::Duration::from_secs(self.duration_secs as u64),
        })
    }
}

impl Default for BackgroundConfig {
    fn default() -> Self {
        Self {
            enable: false,
            compaction: Default::default(),
        }
    }
}

impl Default for BackgroundCompactionConfig {
    fn default() -> Self {
        Self {
            compact_mode: "one_shot".to_string(),
            segment_limit: None,
            block_limit: None,
            fixed_config: Default::default(),
        }
    }
}

impl Debug for BackgroundCompactionConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BackgroundCompactionConfig")
            .field("mode", &self.compact_mode)
            .field("segment_limit", &self.segment_limit)
            .field("block_limit", &self.block_limit)
            .field("fixed_config", &self.fixed_config)
            .finish()
    }
}

impl Default for BackgroundCompactionFixedConfig {
    fn default() -> Self {
        Self {
            duration_secs: 1800,
        }
    }
}

impl Debug for BackgroundCompactionFixedConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
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
                segment_limit: None,
                block_limit: None,
                params: Default::default(),
            },
        }
    }
}

impl Debug for InnerBackgroundConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InnerBackgroundConfig")
            .field("compaction", &self.compaction)
            .finish()
    }
}

impl Debug for InnerBackgroundCompactionConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InnerBackgroundCompactionConfig")
            .field("segment_limit", &self.segment_limit)
            .field("block_limit", &self.block_limit)
            .field("params", &self.params)
            .finish()
    }
}
