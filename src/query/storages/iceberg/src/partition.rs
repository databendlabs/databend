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

use std::any::Any;
use std::hash::DefaultHasher;
use std::hash::Hash;
use std::hash::Hasher;

use databend_common_catalog::plan::PartInfo;
use databend_common_catalog::plan::PartInfoPtr;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct IcebergPartInfo(iceberg::scan::FileScanTask);

impl PartialEq for IcebergPartInfo {
    fn eq(&self, other: &Self) -> bool {
        self.0.data_file_path == other.0.data_file_path
            && self.0.start == other.0.start
            && self.0.length == other.0.length
            && self.0.predicate == other.0.predicate
            && self.0.schema == other.0.schema
            && self.0.project_field_ids == other.0.project_field_ids
    }
}

impl IcebergPartInfo {
    pub fn new(task: iceberg::scan::FileScanTask) -> Self {
        Self(task)
    }

    pub fn from_part(info: &PartInfoPtr) -> Result<&IcebergPartInfo> {
        info.as_any()
            .downcast_ref::<IcebergPartInfo>()
            .ok_or_else(|| ErrorCode::Internal("Cannot downcast from PartInfo to IcebergPartInfo."))
    }

    pub fn to_task(&self) -> iceberg::scan::FileScanTask {
        self.0.clone()
    }
}

#[typetag::serde(name = "iceberg")]
impl PartInfo for IcebergPartInfo {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn equals(&self, info: &Box<dyn PartInfo>) -> bool {
        info.as_any()
            .downcast_ref::<IcebergPartInfo>()
            .is_some_and(|other| self == other)
    }

    fn hash(&self) -> u64 {
        let mut s = DefaultHasher::new();
        self.0.data_file_path.hash(&mut s);
        self.0.start.hash(&mut s);
        self.0.length.hash(&mut s);
        s.finish()
    }
}
