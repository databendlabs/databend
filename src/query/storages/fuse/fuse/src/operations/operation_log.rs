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

use common_exception::ErrorCode;
use common_expression::Chunk;
use common_expression::ChunkMetaInfo;
use common_storages_table_meta::meta::SegmentInfo;

// currently, only support append,
pub type TableOperationLog = Vec<AppendOperationLogEntry>;

// to be wrapped in enum
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct AppendOperationLogEntry {
    pub segment_location: String,
    // pub segment_info: Arc<SegmentInfo>, todo!("expression");
}

impl AppendOperationLogEntry {
    pub fn new(segment_location: String, segment_info: Arc<SegmentInfo>) -> Self {
        todo!("expression");
        // Self {
        //     segment_location,
        //     segment_info,
        // }
    }
}

impl TryFrom<AppendOperationLogEntry> for Chunk {
    type Error = ErrorCode;
    fn try_from(value: AppendOperationLogEntry) -> Result<Self, Self::Error> {
        Ok(Chunk::new_with_meta(
            vec![],
            0,
            Some(Arc::new(Box::new(value))),
        ))
    }
}

impl TryFrom<&Chunk> for AppendOperationLogEntry {
    type Error = ErrorCode;
    fn try_from(chunk: &Chunk) -> Result<Self, Self::Error> {
        let err = ErrorCode::Internal(format!(
            "invalid data block meta of AppendOperation log, {:?}",
            chunk.meta()
        ));

        if let Some(meta) = chunk.meta()? {
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
impl ChunkMetaInfo for AppendOperationLogEntry {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn equals(&self, info: &Box<dyn ChunkMetaInfo>) -> bool {
        match info.as_any().downcast_ref::<AppendOperationLogEntry>() {
            None => false,
            Some(other) => self == other,
        }
    }
}
