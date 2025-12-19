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
use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::Hash;
use std::hash::Hasher;
use std::sync::Arc;

use databend_common_catalog::plan::PartInfo;
use databend_common_catalog::plan::PartInfoPtr;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Scalar;

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq, Debug, Clone)]
pub struct HivePartInfo {
    // file location, like /usr/hive/warehouse/ssb.db/customer.table/c_region=ASIA/c_nation=CHINA/f00.parquet
    pub filename: String,
    // partition values, like 'c_region=ASIA/c_nation=CHINA'
    pub partitions: Vec<Scalar>,
    // file size
    pub filesize: u64,
}

#[typetag::serde(name = "hive")]
impl PartInfo for HivePartInfo {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn equals(&self, info: &Box<dyn PartInfo>) -> bool {
        info.as_any()
            .downcast_ref::<HivePartInfo>()
            .is_some_and(|other| self == other)
    }

    fn hash(&self) -> u64 {
        let mut s = DefaultHasher::new();
        self.filename.hash(&mut s);
        s.finish()
    }
}

impl HivePartInfo {
    pub fn create(filename: String, partitions: Vec<Scalar>, filesize: u64) -> Self {
        HivePartInfo {
            filename,
            partitions,
            filesize,
        }
    }

    pub fn into_part_ptr(self) -> PartInfoPtr {
        Arc::new(Box::new(self))
    }

    pub fn from_part(info: &PartInfoPtr) -> Result<&HivePartInfo> {
        info.as_any()
            .downcast_ref::<HivePartInfo>()
            .ok_or_else(|| ErrorCode::Internal("Cannot downcast from PartInfo to HivePartInfo."))
    }
}

// partitions like 'c_region=ASIA/c_nation=CHINA'
pub fn parse_hive_partitions(partitions: &str) -> HashMap<String, String> {
    let mut partition_map = HashMap::new();
    let parts = partitions.split('/').collect::<Vec<_>>();
    for part in parts {
        let kv = part.split('=').collect::<Vec<_>>();
        if kv.len() == 2 {
            partition_map.insert(kv[0].to_string(), kv[1].to_string());
        }
    }
    partition_map
}
