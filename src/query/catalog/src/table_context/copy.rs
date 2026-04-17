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
use databend_common_storage::CopyStatus;
use databend_common_storage::FileStatus;
use databend_common_storage::StageFileInfo;

use crate::table_context::FilteredCopyFiles;

#[async_trait::async_trait]
pub trait TableContextCopy: Send + Sync {
    async fn filter_out_copied_files(
        &self,
        catalog_name: &str,
        database_name: &str,
        table_name: &str,
        files: &[StageFileInfo],
        path_prefix: Option<String>,
        max_files: Option<usize>,
    ) -> Result<FilteredCopyFiles>;

    fn add_file_status(&self, file_path: &str, file_status: FileStatus) -> Result<()>;

    fn get_copy_status(&self) -> Arc<CopyStatus>;
}
