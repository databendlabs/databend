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

use databend_common_catalog::plan::PartInfoPtr;
use databend_common_expression::BlockMetaInfo;
use databend_common_expression::BlockMetaInfoPtr;
use serde::Deserializer;
use serde::Serializer;

pub struct DataSourceWithMeta<T> {
    // Meta information of data carried.
    // Currently, the concrete type behind the PartInfoPtr is FusePartInfo.
    pub meta: Vec<PartInfoPtr>,
    pub data: Vec<T>,
}

impl<T> DataSourceWithMeta<T>
where DataSourceWithMeta<T>: BlockMetaInfo
{
    pub fn create(part: Vec<PartInfoPtr>, data: Vec<T>) -> BlockMetaInfoPtr {
        {
            let part_len = part.len();
            let data_len = data.len();
            assert_eq!(
                part_len, data_len,
                "Number of PartInfoPtr {} should equal to number of DataSource {}",
                part_len, data_len,
            );
        }

        Box::new(DataSourceWithMeta { meta: part, data })
    }
}

impl<T> Debug for DataSourceWithMeta<T> {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        f.debug_struct("DataSourceWithMeta")
            .field("meta", &self.meta)
            .finish()
    }
}

impl<T> serde::Serialize for DataSourceWithMeta<T> {
    fn serialize<S>(&self, _: S) -> std::result::Result<S::Ok, S::Error>
    where S: Serializer {
        unimplemented!("Unimplemented serialize DataSourceMeta")
    }
}

impl<'de, T> serde::Deserialize<'de> for DataSourceWithMeta<T> {
    fn deserialize<D>(_: D) -> std::result::Result<Self, D::Error>
    where D: Deserializer<'de> {
        unimplemented!("Unimplemented deserialize DataSourceMeta")
    }
}
