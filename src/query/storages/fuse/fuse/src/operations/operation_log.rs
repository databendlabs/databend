//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::any::Any;
use std::convert::TryFrom;
use std::fmt::Debug;
use std::sync::Arc;

use common_datablocks::BlockMetaInfo;
use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_storages_table_meta::meta::SegmentInfo;

// currently, only support append,
pub type TableOperationLog = Vec<AppendOperationLogEntry>;

// to be wrapped in enum
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct AppendOperationLogEntry {
    pub segment_location: String,
    pub segment_info: Arc<SegmentInfo>,
}

impl AppendOperationLogEntry {
    pub fn new(segment_location: String, segment_info: Arc<SegmentInfo>) -> Self {
        Self {
            segment_location,
            segment_info,
        }
    }
}

impl TryFrom<AppendOperationLogEntry> for DataBlock {
    type Error = ErrorCode;
    fn try_from(value: AppendOperationLogEntry) -> Result<Self, Self::Error> {
        Ok(DataBlock::create_with_meta(
            DataSchemaRef::default(),
            vec![],
            Some(Arc::new(Box::new(value))),
        ))
    }
}

impl TryFrom<&DataBlock> for AppendOperationLogEntry {
    type Error = ErrorCode;
    fn try_from(block: &DataBlock) -> Result<Self, Self::Error> {
        let err = ErrorCode::Internal(format!(
            "invalid data block meta of AppendOperation log, {:?}",
            block.meta()
        ));

        if let Some(meta) = block.meta()? {
            let cast = meta.as_any().downcast_ref::<AppendOperationLogEntry>();
            return match cast {
                None => Err(err),
                Some(entry) => Ok(entry.clone()),
            };
        }

        Err(err)
    }
}

#[typetag::serde(name = "operation_log")]
impl BlockMetaInfo for AppendOperationLogEntry {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn equals(&self, info: &Box<dyn BlockMetaInfo>) -> bool {
        match info.as_any().downcast_ref::<AppendOperationLogEntry>() {
            None => false,
            Some(other) => self == other,
        }
    }
}
