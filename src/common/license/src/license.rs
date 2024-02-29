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
use strum::IntoEnumIterator;
use strum_macros::EnumIter;

// All enterprise features are defined here.
#[derive(Debug, PartialEq, EnumIter)]
pub enum Feature {
    LicenseInfo,
    Vacuum,
    Test,
    VirtualColumn,
    BackgroundService,
    DataMask,
    AggregateIndex,
    ComputedColumn,
    StorageEncryption,
    Stream,
}

impl Display for Feature {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Feature::VirtualColumn => {
                write!(f, "virtual_column")
            }
            Feature::LicenseInfo => {
                write!(f, "license_info")
            }
            Feature::Vacuum => {
                write!(f, "vacuum")
            }
            Feature::Test => {
                write!(f, "test")
            }
            Feature::BackgroundService => {
                write!(f, "background_service")
            }
            Feature::DataMask => {
                write!(f, "data_mask")
            }
            Feature::AggregateIndex => {
                write!(f, "aggregate_index")
            }
            Feature::ComputedColumn => {
                write!(f, "computed_column")
            }
            Feature::StorageEncryption => {
                write!(f, "storage_encryption")
            }
            Feature::Stream => {
                write!(f, "stream")
            }
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct LicenseInfo {
    #[serde(rename = "type")]
    pub r#type: Option<String>,
    pub org: Option<String>,
    pub tenants: Option<Vec<String>>,
    pub features: Option<Vec<String>>,
}

impl LicenseInfo {
    pub fn display_features(&self) -> String {
        // sort all features in alphabet order and ignore test feature
        let mut binding = self.features.clone().unwrap_or_default();
        if binding.is_empty() {
            binding = Feature::iter().map(|f| f.to_string()).collect::<Vec<_>>();
        }
        let mut features = binding
            .iter()
            .filter(|f| *f != &Feature::Test.to_string())
            .collect::<Vec<_>>();
        features.sort();
        features
            .iter()
            .map(|f| f.to_string())
            .collect::<Vec<_>>()
            .join(",")
    }
}
