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

use std::fmt::Display;
use std::fmt::Formatter;

use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Eq, Ord, PartialOrd, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ComputeQuota {
    threads_num: Option<usize>,
    memory_usage: Option<usize>,
}

#[derive(Debug, Clone, Eq, Ord, PartialOrd, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct StorageQuota {
    pub storage_usage: Option<usize>,
}

/// We allow user to use upto 1TiB storage size.
impl Default for StorageQuota {
    fn default() -> Self {
        Self {
            storage_usage: Some(1024 * 1024 * 1024 * 1024),
        }
    }
}

// All enterprise features are defined here.
#[derive(Debug, Clone, Eq, Ord, PartialOrd, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum Feature {
    #[serde(alias = "license_info", alias = "LICENSE_INFO")]
    LicenseInfo,
    #[serde(alias = "vacuum", alias = "VACUUM")]
    Vacuum,
    #[serde(alias = "test", alias = "TEST")]
    Test,
    #[serde(alias = "virtual_column", alias = "VIRTUAL_COLUMN")]
    VirtualColumn,
    #[serde(alias = "background_service", alias = "BACKGROUND_SERVICE")]
    BackgroundService,
    #[serde(alias = "data_mask", alias = "DATA_MASK")]
    DataMask,
    #[serde(alias = "aggregate_index", alias = "AGGREGATE_INDEX")]
    AggregateIndex,
    #[serde(alias = "inverted_index", alias = "INVERTED_INDEX")]
    InvertedIndex,
    #[serde(alias = "computed_column", alias = "COMPUTED_COLUMN")]
    ComputedColumn,
    #[serde(alias = "storage_encryption", alias = "STORAGE_ENCRYPTION")]
    StorageEncryption,
    #[serde(alias = "stream", alias = "STREAM")]
    Stream,
    #[serde(alias = "compute_quota", alias = "COMPUTE_QUOTA")]
    ComputeQuota(ComputeQuota),
    #[serde(alias = "storage_quota", alias = "STORAGE_QUOTA")]
    StorageQuota(StorageQuota),
    #[serde(other)]
    Unknown,
}

impl Display for Feature {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            Feature::LicenseInfo => write!(f, "license_info"),
            Feature::Vacuum => write!(f, "vacuum"),
            Feature::Test => write!(f, "test"),
            Feature::VirtualColumn => write!(f, "virtual_column"),
            Feature::BackgroundService => write!(f, "background_service"),
            Feature::DataMask => write!(f, "data_mask"),
            Feature::AggregateIndex => write!(f, "aggregate_index"),
            Feature::InvertedIndex => write!(f, "inverted_index"),
            Feature::ComputedColumn => write!(f, "computed_column"),
            Feature::StorageEncryption => write!(f, "storage_encryption"),
            Feature::Stream => write!(f, "stream"),
            Feature::ComputeQuota(v) => {
                write!(f, "compute_quota(")?;

                match &v.threads_num {
                    None => write!(f, "threads_num: unlimited,")?,
                    Some(threads_num) => write!(f, "threads_num: {}", *threads_num)?,
                };

                match v.memory_usage {
                    None => write!(f, "memory_usage: unlimited,"),
                    Some(memory_usage) => write!(f, "memory_usage: {}", memory_usage),
                }
            }
            Feature::StorageQuota(v) => {
                write!(f, "storage_quota(")?;

                match v.storage_usage {
                    None => write!(f, "storage_usage: unlimited,"),
                    Some(storage_usage) => write!(f, "storage_usage: {}", storage_usage),
                }
            }
            Feature::Unknown => write!(f, "unknown"),
        }
    }
}

impl Feature {
    pub fn verify(&self, feature: &Feature) -> bool {
        match (self, feature) {
            (Feature::ComputeQuota(c), Feature::ComputeQuota(v)) => {
                if let Some(thread_num) = c.threads_num {
                    if thread_num <= v.threads_num.unwrap_or(usize::MAX) {
                        return false;
                    }
                }

                if let Some(max_memory_usage) = c.memory_usage {
                    if max_memory_usage <= v.memory_usage.unwrap_or(usize::MAX) {
                        return false;
                    }
                }

                true
            }
            (Feature::StorageQuota(c), Feature::StorageQuota(v)) => {
                if let Some(max_storage_usage) = c.storage_usage {
                    if max_storage_usage <= v.storage_usage.unwrap_or(usize::MAX) {
                        return false;
                    }
                }

                true
            }
            (Feature::Test, Feature::Test)
            | (Feature::AggregateIndex, Feature::AggregateIndex)
            | (Feature::ComputedColumn, Feature::ComputedColumn)
            | (Feature::Vacuum, Feature::Vacuum)
            | (Feature::LicenseInfo, Feature::LicenseInfo)
            | (Feature::Stream, Feature::Stream)
            | (Feature::BackgroundService, Feature::BackgroundService)
            | (Feature::DataMask, Feature::DataMask)
            | (Feature::InvertedIndex, Feature::InvertedIndex)
            | (Feature::VirtualColumn, Feature::VirtualColumn)
            | (Feature::StorageEncryption, Feature::StorageEncryption) => true,
            (_, _) => false,
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct LicenseInfo {
    #[serde(rename = "type")]
    pub r#type: Option<String>,
    pub org: Option<String>,
    pub tenants: Option<Vec<String>>,
    pub features: Option<Vec<Feature>>,
}

impl LicenseInfo {
    pub fn display_features(&self) -> String {
        // sort all features in alphabet order and ignore test feature
        let mut features = self.features.clone().unwrap_or_default();

        if features.is_empty() {
            return String::from("Unlimited");
        }

        features.sort();

        features
            .iter()
            .filter(|f| **f != Feature::Test)
            .map(|f| f.to_string())
            .collect::<Vec<_>>()
            .join(",")
    }

    /// Get Storage Quota from given license info.
    ///
    /// Returns the default storage quota if the storage quota is not licensed.
    pub fn get_storage_quota(&self) -> StorageQuota {
        let Some(features) = self.features.as_ref() else {
            return StorageQuota::default();
        };

        features
            .iter()
            .find_map(|f| match f {
                Feature::StorageQuota(v) => Some(v),
                _ => None,
            })
            .cloned()
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialize_feature_from_string() {
        assert_eq!(
            Feature::LicenseInfo,
            serde_json::from_str::<Feature>("\"license_info\"").unwrap()
        );
        assert_eq!(
            Feature::Vacuum,
            serde_json::from_str::<Feature>("\"Vacuum\"").unwrap()
        );
        assert_eq!(
            Feature::Test,
            serde_json::from_str::<Feature>("\"Test\"").unwrap()
        );
        assert_eq!(
            Feature::VirtualColumn,
            serde_json::from_str::<Feature>("\"VIRTUAL_COLUMN\"").unwrap()
        );
        assert_eq!(
            Feature::BackgroundService,
            serde_json::from_str::<Feature>("\"BackgroundService\"").unwrap()
        );
        assert_eq!(
            Feature::DataMask,
            serde_json::from_str::<Feature>("\"DataMask\"").unwrap()
        );
        assert_eq!(
            Feature::AggregateIndex,
            serde_json::from_str::<Feature>("\"AggregateIndex\"").unwrap()
        );
        assert_eq!(
            Feature::InvertedIndex,
            serde_json::from_str::<Feature>("\"InvertedIndex\"").unwrap()
        );
        assert_eq!(
            Feature::ComputedColumn,
            serde_json::from_str::<Feature>("\"ComputedColumn\"").unwrap()
        );
        assert_eq!(
            Feature::StorageEncryption,
            serde_json::from_str::<Feature>("\"StorageEncryption\"").unwrap()
        );
        assert_eq!(
            Feature::Stream,
            serde_json::from_str::<Feature>("\"Stream\"").unwrap()
        );
        assert_eq!(
            Feature::ComputeQuota(ComputeQuota {
                threads_num: Some(1),
                memory_usage: Some(1),
            }),
            serde_json::from_str::<Feature>(
                "{\"ComputeQuota\":{\"threads_num\":1, \"memory_usage\":1}}"
            )
            .unwrap()
        );

        assert_eq!(
            Feature::ComputeQuota(ComputeQuota {
                threads_num: None,
                memory_usage: Some(1),
            }),
            serde_json::from_str::<Feature>("{\"ComputeQuota\":{\"memory_usage\":1}}").unwrap()
        );

        assert_eq!(
            Feature::StorageQuota(StorageQuota {
                storage_usage: Some(1),
            }),
            serde_json::from_str::<Feature>("{\"StorageQuota\":{\"storage_usage\":1}}").unwrap()
        );

        assert_eq!(
            Feature::Unknown,
            serde_json::from_str::<Feature>("\"ssss\"").unwrap()
        );
    }
}
