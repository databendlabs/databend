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

use databend_common_exception::Result;
use databend_common_meta_app::principal::UserDefinedFileFormat;
use databend_common_meta_app::schema::OnExist;
use databend_common_meta_types::MatchSeq;
use databend_common_meta_types::SeqV;

#[async_trait::async_trait]
pub trait FileFormatApi: Sync + Send {
    // Add a file_format info to /tenant/file_format-name.
    async fn add_file_format(
        &self,
        file_format: UserDefinedFileFormat,
        create_option: &OnExist,
    ) -> Result<()>;

    async fn get_file_format(
        &self,
        name: &str,
        seq: MatchSeq,
    ) -> Result<SeqV<UserDefinedFileFormat>>;

    // Get all the file_formats for a tenant.
    async fn get_file_formats(&self) -> Result<Vec<UserDefinedFileFormat>>;

    // Drop the tenant's file_format by name.
    async fn drop_file_format(&self, name: &str, seq: MatchSeq) -> Result<()>;
}
