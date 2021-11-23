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
use common_datavalues::DataField;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataSchemaRefExt;
use common_datavalues::DataType;
use common_datavalues::DataValue;
use common_exception::ErrorCode;

use crate::datasources::table::fuse::SegmentInfo;

// currently, only support append
pub type TableOperationLog = Vec<AppendOperation>;

pub struct AppendOperation {
    pub segment_location: String,
    pub segment_info: SegmentInfo,
}

impl AppendOperation {
    pub fn schema() -> DataSchemaRef {
        // CALL_ONCE / lazy_init?
        DataSchemaRefExt::create(vec![
            DataField::new("seg_loc", DataType::String, false),
            DataField::new("seg_info", DataType::String, false),
        ])
    }

    pub fn new(segment_location: String, segment_info: SegmentInfo) -> Self {
        Self {
            segment_location,
            segment_info,
        }
    }
}

// TODO refine this, json is not that suitable
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
    fn try_from(value: &DataBlock) -> std::result::Result<Self, Self::Error> {
        // check schema
        if value.schema() != &AppendOperation::schema() {
            return Err(ErrorCode::LogicalError(format!(
                "invalid data block of AppendOperation log, {:?}",
                value.schema()
            )));
        }

        let string_value_of = |idx, val: &DataBlock| {
            let col = &val.column(idx).to_values()?[0];
            if let DataValue::String(Some(v)) = col {
                Ok(String::from_utf8(v.clone())?)
            } else {
                Err(ErrorCode::LogicalError(
                    "can not extract string values from Append Operation log",
                ))
            }
        };

        let segment_location = string_value_of(0, value)?;
        let seg_info = string_value_of(1, value)?;
        let segment_info: SegmentInfo = serde_json::from_str(seg_info.as_str())?;
        Ok(AppendOperation {
            segment_location,
            segment_info,
        })
    }
}
