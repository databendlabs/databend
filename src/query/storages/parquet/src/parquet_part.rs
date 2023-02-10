// Copyright 2022 Datafuse Labs.
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
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::Hash;
use std::hash::Hasher;
use std::sync::Arc;

use common_arrow::parquet::compression::Compression;
use common_arrow::parquet::indexes::Interval;
use common_catalog::plan::PartInfo;
use common_catalog::plan::PartInfoPtr;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::Scalar;

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct ColumnMeta {
    pub offset: u64,
    pub length: u64,
    pub compression: Compression,
    pub min_max: Option<(Scalar, Scalar)>,

    // if has dictionary, we can not push down predicate to deserialization.
    pub has_dictionary: bool,
}

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct ParquetRowGroupPart {
    pub location: String,
    pub num_rows: usize,
    pub column_metas: HashMap<usize, ColumnMeta>,
    pub row_selection: Option<Vec<Interval>>,

    pub sort_min_max: Option<(Scalar, Scalar)>,
}

impl ParquetRowGroupPart {
    pub fn convert_to_part_info(self) -> PartInfoPtr {
        Arc::new(Box::new(self))
    }
}

#[typetag::serde(name = "parquet_row_group")]
impl PartInfo for ParquetRowGroupPart {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn equals(&self, info: &Box<dyn PartInfo>) -> bool {
        match info.as_any().downcast_ref::<ParquetRowGroupPart>() {
            None => false,
            Some(other) => self == other,
        }
    }

    fn hash(&self) -> u64 {
        let mut s = DefaultHasher::new();
        self.location.hash(&mut s);
        s.finish()
    }
}

impl ParquetRowGroupPart {
    pub fn from_part(info: &PartInfoPtr) -> Result<&ParquetRowGroupPart> {
        match info.as_any().downcast_ref::<ParquetRowGroupPart>() {
            Some(part_ref) => Ok(part_ref),
            None => Err(ErrorCode::Internal(
                "Cannot downcast from PartInfo to ParquetRowGroupPart.",
            )),
        }
    }
}
