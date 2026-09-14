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

use std::str::FromStr;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::principal::FileFormatOptionsReader;
use databend_common_meta_app::principal::FileFormatParams;
use databend_common_meta_app::principal::StageFileFormatType;
use databend_common_meta_app::tenant::Tenant;
use databend_common_users::UserApiProvider;

pub async fn resolve_file_format(
    tenant: &Tenant,
    user_api: &UserApiProvider,
    name: &str,
) -> Result<FileFormatParams> {
    let params = match StageFileFormatType::from_str(name) {
        Ok(typ) => FileFormatParams::default_by_type(typ),
        Err(_) => Ok(user_api
            .get_file_format(tenant, name)
            .await?
            .file_format_params),
    }?;
    ensure_query_file_format_supported(&params)?;
    Ok(params)
}

pub fn parse_file_format(reader: FileFormatOptionsReader) -> Result<FileFormatParams> {
    let params = FileFormatParams::try_from_reader(reader, false)?;
    ensure_query_file_format_supported(&params)?;
    Ok(params)
}

pub(crate) fn ensure_query_file_format_supported(params: &FileFormatParams) -> Result<()> {
    if matches!(params, FileFormatParams::Lance(_)) {
        return Err(ErrorCode::IllegalFileFormat(
            "LANCE file format is unsupported".to_string(),
        ));
    }
    Ok(())
}
