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

use common_exception::Result;
use common_meta_types::StageType;
use common_meta_types::UserStageInfo;
use common_storage::init_operator;
use opendal::Operator;

use crate::sessions::TableContext;

pub struct StageSourceHelper;
impl StageSourceHelper {
    /// TODO: we should support construct operator with
    /// correct root.
    pub async fn get_op(ctx: &Arc<dyn TableContext>, stage: &UserStageInfo) -> Result<Operator> {
        if stage.stage_type == StageType::Internal {
            ctx.get_storage_operator()
        } else {
            Ok(init_operator(&stage.stage_params.storage)?)
        }
    }
}
