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

use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_meta_app::principal::StageInfo;
use databend_common_pipeline_core::Pipeline;
use databend_common_settings::Settings;
use databend_common_storage::StageFileInfo;
use opendal::Operator;

use crate::input_formats::InputContext;
use crate::input_formats::SplitInfo;

#[async_trait::async_trait]
pub trait InputFormat: Send + Sync {
    async fn get_splits(
        &self,
        file_infos: Vec<StageFileInfo>,
        stage_info: &StageInfo,
        op: &Operator,
        settings: &Arc<Settings>,
    ) -> Result<Vec<Arc<SplitInfo>>>;

    fn exec_copy(&self, ctx: Arc<InputContext>, pipeline: &mut Pipeline) -> Result<()>;

    fn exec_stream(&self, ctx: Arc<InputContext>, pipeline: &mut Pipeline) -> Result<()>;
}
