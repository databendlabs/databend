//  Copyright 2022 Datafuse Labs.
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

use std::any::Any;
use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::PartInfo;
use common_planners::PartInfoPtr;

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct NumbersPartInfo {
    pub total: u64,
    pub part_start: u64,
    pub part_end: u64,
}

#[typetag::serde(name = "numbers")]
impl PartInfo for NumbersPartInfo {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn equals(&self, info: &Box<dyn PartInfo>) -> bool {
        match info.as_any().downcast_ref::<NumbersPartInfo>() {
            None => false,
            Some(other) => self == other,
        }
    }
}

impl NumbersPartInfo {
    pub fn create(start: u64, end: u64, total: u64) -> Arc<Box<dyn PartInfo>> {
        Arc::new(Box::new(NumbersPartInfo {
            total,
            part_start: start,
            part_end: end,
        }))
    }

    pub fn from_part(info: &PartInfoPtr) -> Result<&NumbersPartInfo> {
        match info.as_any().downcast_ref::<NumbersPartInfo>() {
            Some(part_ref) => Ok(part_ref),
            None => Err(ErrorCode::LogicalError(
                "Cannot downcast from PartInfo to NumbersPartInfo.",
            )),
        }
    }
}
