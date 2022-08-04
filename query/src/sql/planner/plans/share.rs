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

use std::sync::Arc;

use common_datavalues::chrono::Utc;
use common_datavalues::DataSchema;
use common_datavalues::DataSchemaRef;
use common_meta_app::share::CreateShareReq;
use common_meta_app::share::ShareNameIdent;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct CreateSharePlan {
    pub if_not_exists: bool,
    pub tenant: String,
    pub share: String,
    pub comment: Option<String>,
}

impl From<CreateSharePlan> for CreateShareReq {
    fn from(p: CreateSharePlan) -> Self {
        CreateShareReq {
            if_not_exists: p.if_not_exists,
            share_name: ShareNameIdent {
                tenant: p.tenant,
                share_name: p.share,
            },
            comment: p.comment,
            create_on: Utc::now(),
        }
    }
}

impl CreateSharePlan {
    pub fn schema(&self) -> DataSchemaRef {
        Arc::new(DataSchema::empty())
    }
}
