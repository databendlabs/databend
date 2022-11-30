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


use common_datavalues::prelude::*;
use common_meta_types::UserStageInfo;
use crate::{NameAndDataType, NameAndDataTypes};

#[derive(PartialEq, Eq, Clone, Debug)]
pub struct ListPlan {
    pub stage: UserStageInfo,
    pub path: String,
    pub pattern: String,
}

impl ListPlan {
    pub fn schema(&self) -> NameAndDataTypes {
        let name = NameAndDataType::new("name", Vu8::to_data_type());
        let size = NameAndDataType::new("size", u64::to_data_type());
        let md5 = NameAndDataType::new("md5", wrap_nullable(&Vu8::to_data_type()));
        let last_modified = NameAndDataType::new("last_modified", Vu8::to_data_type());
        let creator = NameAndDataType::new("creator", wrap_nullable(&Vu8::to_data_type()));

        NameAndDataTypes::new(vec![
            name,
            size,
            md5,
            last_modified,
            creator,
        ])
    }
}
