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
//

use std::convert::TryFrom;

use common_datablocks::DataBlock;
use common_datavalues::series::Series;
use common_datavalues::series::SeriesFrom;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataValue;
use common_exception::ErrorCode;

use crate::datasources::table::fuse::SegmentInfo;

// currently, only support append,
pub type TableOperationLog = Vec<AppendOperation>;

// to be wrapped in enum
pub struct AppendOperation {
    pub segment_location: String,
    pub segment_info: SegmentInfo,
}

impl AppendOperation {
    pub fn schema() -> DataSchemaRef {
        common_planners::SINK_SCHEMA.clone()
    }

    pub fn new(segment_location: String, segment_info: SegmentInfo) -> Self {
        Self {
            segment_location,
            segment_info,
        }
    }
}

impl TryFrom<AppendOperation> for DataBlock {
    type Error = common_exception::ErrorCode;
    fn try_from(value: AppendOperation) -> std::result::Result<Self, Self::Error> {
        Ok(DataBlock::create_by_array(AppendOperation::schema(), vec![
            Series::new(vec![value.segment_location.as_str()]),
            Series::new(vec![serde_json::to_string(&value.segment_info)?.as_str()]),
        ]))
    }
}

impl TryFrom<&DataBlock> for AppendOperation {
    type Error = common_exception::ErrorCode;
    fn try_from(block: &DataBlock) -> std::result::Result<Self, Self::Error> {
        // check schema
        if block.schema() != &AppendOperation::schema() {
            return Err(ErrorCode::LogicalError(format!(
                "invalid data block of AppendOperation log, {:?}",
                block.schema()
            )));
        }

        let segment_location = Self::parse_col(0, block)?;
        let seg_info = Self::parse_col(1, block)?;
        let segment_info: SegmentInfo = serde_json::from_str(seg_info.as_str())?;
        Ok(AppendOperation {
            segment_location,
            segment_info,
        })
    }
}

impl AppendOperation {
    fn parse_col(idx: usize, val: &DataBlock) -> common_exception::Result<String> {
        let col = &val.column(idx).to_values()?[0];
        if let DataValue::String(Some(v)) = col {
            Ok(String::from_utf8(v.clone())?)
        } else {
            Err(ErrorCode::LogicalError(format!(
                "can not extract string value from data block as \
                 a column of Append Operation log (col: {})",
                idx
            )))
        }
    }
}
