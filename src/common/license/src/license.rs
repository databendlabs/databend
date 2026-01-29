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

use std::fmt;

use databend_common_exception::ErrorCode;
use display_more::DisplayOptionExt;
use display_more::DisplaySliceExt;
use serde::Deserialize;
use serde::Serialize;

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
    #[serde(alias = "data_mask", alias = "DATA_MASK")]
    DataMask,
    #[serde(alias = "computed_column", alias = "COMPUTED_COLUMN")]
    ComputedColumn,
    #[serde(alias = "storage_encryption", alias = "STORAGE_ENCRYPTION")]
    StorageEncryption,
    #[serde(alias = "stream", alias = "STREAM")]
    Stream,
    #[serde(alias = "table_ref", alias = "TABLE_REF")]
    TableRef,
    #[serde(alias = "attach_table", alias = "ATTACH_TABLE")]
    AttacheTable,
    #[serde(alias = "amend_table", alias = "AMEND_TABLE")]
    AmendTable,
    #[serde(alias = "hilbert_clustering", alias = "HILBERT_CLUSTERING")]
    HilbertClustering,
    #[serde(alias = "system_management", alias = "SYSTEM_MANAGEMENT")]
    SystemManagement,
    #[serde(alias = "workload_group", alias = "WORKLOAD_GROUP")]
    WorkloadGroup,
    #[serde(alias = "system_history", alias = "SYSTEM_HISTORY")]
    SystemHistory,
    #[serde(alias = "private_task", alias = "PRIVATE_TASK")]
    PrivateTask,
    #[serde(alias = "max_node_quota", alias = "MAX_NODE_QUOTA")]
    MaxNodeQuota(usize),
    #[serde(alias = "max_cpu_quota", alias = "MAX_CPU_QUOTA")]
    MaxCpuQuota(usize),
    #[serde(alias = "row_access_policy", alias = "ROW_ACCESS_POLICY")]
    RowAccessPolicy,
    #[serde(other)]
    Unknown,
}

pub enum VerifyResult {
    MissMatch,
    Success,
    Failure,
}

impl fmt::Display for Feature {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Feature::LicenseInfo => write!(f, "license_info"),
            Feature::Vacuum => write!(f, "vacuum"),
            Feature::Test => write!(f, "test"),
            Feature::VirtualColumn => write!(f, "virtual_column"),
            Feature::DataMask => write!(f, "data_mask"),
            Feature::ComputedColumn => write!(f, "computed_column"),
            Feature::StorageEncryption => write!(f, "storage_encryption"),
            Feature::Stream => write!(f, "stream"),
            Feature::TableRef => write!(f, "table_ref"),
            Feature::AttacheTable => write!(f, "attach_table"),
            Feature::AmendTable => write!(f, "amend_table"),
            Feature::SystemManagement => write!(f, "system_management"),
            Feature::HilbertClustering => write!(f, "hilbert_clustering"),
            Feature::WorkloadGroup => write!(f, "workload_group"),
            Feature::SystemHistory => write!(f, "system_history"),
            Feature::PrivateTask => write!(f, "private_task"),
            Feature::RowAccessPolicy => write!(f, "row_access_policy"),
            Feature::Unknown => write!(f, "unknown"),
            Feature::MaxCpuQuota(v) => write!(f, "max_cpu_quota({})", v),
            Feature::MaxNodeQuota(v) => write!(f, "max_node_quota({})", v),
        }
    }
}

impl Feature {
    pub fn verify_default(&self, message: impl Into<String>) -> Result<(), ErrorCode> {
        match self {
            Feature::MaxCpuQuota(_) | Feature::MaxNodeQuota(_) => Ok(()),
            _ => Err(ErrorCode::LicenseKeyInvalid(message.into())),
        }
    }

    pub fn verify(&self, feature: &Feature) -> Result<VerifyResult, ErrorCode> {
        match (self, feature) {
            (Feature::MaxCpuQuota(c), Feature::MaxCpuQuota(v)) => match c > v {
                true => Ok(VerifyResult::Success),
                false => Ok(VerifyResult::Failure),
            },
            (Feature::MaxNodeQuota(c), Feature::MaxNodeQuota(v)) => match c > v {
                true => Ok(VerifyResult::Success),
                false => Ok(VerifyResult::Failure),
            },
            _ if std::mem::discriminant(self) == std::mem::discriminant(feature) => {
                Ok(VerifyResult::Success)
            }
            _ => Ok(VerifyResult::MissMatch),
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct LicenseInfo {
    #[serde(rename = "type")]
    pub r#type: Option<String>,
    pub org: Option<String>,
    pub tenants: Option<Vec<String>>,
    pub features: Option<Vec<Feature>>,
}

impl fmt::Display for LicenseInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "LicenseInfo{{ type: {}, org: {}, tenants: {}, features: [{}] }}",
            self.r#type.display(),
            self.org.display(),
            self.tenants
                .as_ref()
                .map(|x| x.as_slice().display())
                .display(),
            self.display_features()
        )
    }
}

impl LicenseInfo {
    pub fn display_features(&self) -> impl fmt::Display + '_ {
        /// sort all features in alphabet order and ignore test feature
        struct DisplayFeatures<'a>(&'a LicenseInfo);

        impl fmt::Display for DisplayFeatures<'_> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                let Some(features) = self.0.features.clone() else {
                    return write!(f, "Unlimited");
                };

                let mut features = features
                    .into_iter()
                    .filter(|f| f != &Feature::Test)
                    .map(|x| x.to_string())
                    .collect::<Vec<_>>();

                features.sort();

                for (i, feat) in features.into_iter().enumerate() {
                    if i > 0 {
                        write!(f, ",")?;
                    }

                    write!(f, "{}", feat)?;
                }
                Ok(())
            }
        }

        DisplayFeatures(self)
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
            Feature::DataMask,
            serde_json::from_str::<Feature>("\"DataMask\"").unwrap()
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
            Feature::TableRef,
            serde_json::from_str::<Feature>("\"TableRef\"").unwrap()
        );
        assert_eq!(
            Feature::AttacheTable,
            serde_json::from_str::<Feature>("\"ATTACH_TABLE\"").unwrap()
        );

        assert_eq!(
            Feature::AmendTable,
            serde_json::from_str::<Feature>("\"amend_table\"").unwrap()
        );

        assert_eq!(
            Feature::HilbertClustering,
            serde_json::from_str::<Feature>("\"hilbert_clustering\"").unwrap()
        );

        assert_eq!(
            Feature::WorkloadGroup,
            serde_json::from_str::<Feature>("\"workload_group\"").unwrap()
        );

        assert_eq!(
            Feature::SystemHistory,
            serde_json::from_str::<Feature>("\"system_history\"").unwrap()
        );

        assert_eq!(
            Feature::PrivateTask,
            serde_json::from_str::<Feature>("\"private_task\"").unwrap()
        );

        assert_eq!(
            Feature::MaxNodeQuota(1),
            serde_json::from_str::<Feature>("{\"MaxNodeQuota\": 1}").unwrap()
        );

        assert_eq!(
            Feature::RowAccessPolicy,
            serde_json::from_str::<Feature>("\"RowAccessPolicy\"").unwrap()
        );

        assert_eq!(
            Feature::Unknown,
            serde_json::from_str::<Feature>("\"ssss\"").unwrap()
        );
    }

    #[test]
    fn test_display_license_info() {
        let license_info = LicenseInfo {
            r#type: Some("enterprise".to_string()),
            org: Some("databend".to_string()),
            tenants: Some(vec!["databend_tenant".to_string(), "foo".to_string()]),
            features: Some(vec![
                Feature::LicenseInfo,
                Feature::Vacuum,
                Feature::Test,
                Feature::VirtualColumn,
                Feature::DataMask,
                Feature::ComputedColumn,
                Feature::StorageEncryption,
                Feature::Stream,
                Feature::TableRef,
                Feature::AttacheTable,
                Feature::AmendTable,
                Feature::HilbertClustering,
                Feature::WorkloadGroup,
                Feature::SystemHistory,
                Feature::PrivateTask,
                Feature::RowAccessPolicy,
            ]),
        };

        assert_eq!(
            "LicenseInfo{ type: enterprise, org: databend, tenants: [databend_tenant,foo], features: [amend_table,attach_table,computed_column,data_mask,hilbert_clustering,license_info,private_task,row_access_policy,storage_encryption,stream,system_history,table_ref,vacuum,virtual_column,workload_group] }",
            license_info.to_string()
        );
    }

    #[test]
    fn test_verify_feature_match() {
        let carry_data = vec![Feature::MaxNodeQuota(3), Feature::MaxCpuQuota(16)];
        let normal = vec![
            Feature::LicenseInfo,
            Feature::Vacuum,
            Feature::Test,
            Feature::VirtualColumn,
            Feature::DataMask,
            Feature::ComputedColumn,
            Feature::StorageEncryption,
            Feature::Stream,
            Feature::TableRef,
            Feature::AttacheTable,
            Feature::AmendTable,
            Feature::HilbertClustering,
            Feature::WorkloadGroup,
            Feature::SystemHistory,
            Feature::PrivateTask,
            Feature::RowAccessPolicy,
        ];
        for carry_feature in &carry_data {
            for normal_feature in &normal {
                assert!(matches!(
                    carry_feature.verify(normal_feature).unwrap(),
                    VerifyResult::MissMatch
                ));
                assert!(matches!(
                    normal_feature.verify(carry_feature).unwrap(),
                    VerifyResult::MissMatch
                ));
            }
        }

        for normal_feature in &normal {
            assert!(matches!(
                normal_feature.verify(normal_feature).unwrap(),
                VerifyResult::Success
            ));
        }

        let cpu_quota = Feature::MaxCpuQuota(16);
        assert!(matches!(
            cpu_quota.verify(&Feature::MaxCpuQuota(8)).unwrap(),
            VerifyResult::Success
        ));
        assert!(matches!(
            cpu_quota.verify(&Feature::MaxCpuQuota(16)).unwrap(),
            VerifyResult::Failure
        ));

        let node_quota = Feature::MaxNodeQuota(3);
        assert!(matches!(
            node_quota.verify(&Feature::MaxNodeQuota(1)).unwrap(),
            VerifyResult::Success
        ));
        assert!(matches!(
            node_quota.verify(&Feature::MaxNodeQuota(3)).unwrap(),
            VerifyResult::Failure
        ));
    }
}
