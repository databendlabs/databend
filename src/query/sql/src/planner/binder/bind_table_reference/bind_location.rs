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

use databend_common_ast::ast::Connection;
use databend_common_ast::ast::FileLocation;
use databend_common_ast::ast::SelectStageOptions;
use databend_common_ast::ast::TableAlias;
use databend_common_ast::ast::UriLocation;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::principal::FileFormatParams;
use databend_common_meta_app::principal::StageFileFormatType;
use databend_common_storage::StageFilesInfo;
use databend_common_users::UserApiProvider;

use crate::BindContext;
use crate::binder::Binder;
use crate::binder::StagePathAccess;
use crate::binder::StageResolver;
use crate::binder::resolve_file_format;
use crate::binder::validate_stage_files_path_traversal;
use crate::optimizer::ir::SExpr;

impl Binder {
    /// Bind a location.
    pub(crate) fn bind_location(
        &mut self,
        bind_context: &mut BindContext,
        location: &FileLocation,
        options: &SelectStageOptions,
        alias: &Option<TableAlias>,
    ) -> Result<(SExpr, BindContext)> {
        databend_common_base::runtime::block_on(async move {
            let location = match location {
                FileLocation::Uri(uri) => FileLocation::Uri(UriLocation {
                    connection: Connection::new(options.connection.clone()),
                    ..uri.clone()
                }),
                _ => location.clone(),
            };

            let user_api = UserApiProvider::instance();
            let (mut stage_info, path) = StageResolver::from_table_context(
                self.ctx.clone(),
                user_api.clone(),
                databend_common_config::GlobalConfig::instance()
                    .storage
                    .allow_insecure,
            )?
            .resolve_file_location(&location, StagePathAccess::Read)
            .await?;

            if let Some(f) = &options.file_format {
                stage_info.file_format_params = match StageFileFormatType::from_str(f) {
                    Ok(t) => {
                        if matches!(t, StageFileFormatType::Lance) {
                            return Err(ErrorCode::IllegalFileFormat(
                                "LANCE file format is only supported in COPY INTO <location>"
                                    .to_string(),
                            ));
                        }
                        FileFormatParams::default_by_type(t)?
                    }
                    _ => {
                        let tenant = self.ctx.get_tenant();
                        resolve_file_format(&tenant, &user_api, f).await?
                    }
                }
            }
            let pattern = match &options.pattern {
                None => None,
                Some(pattern) => Some(Self::resolve_copy_pattern(self.ctx.clone(), pattern)?),
            };

            let files_info = StageFilesInfo {
                path,
                pattern,
                files: options.files.clone(),
            };
            validate_stage_files_path_traversal(
                self.ctx.get_settings().as_ref(),
                &files_info.path,
                files_info.files.as_deref(),
                false,
            )?;
            let table_ctx = self.ctx.clone();
            self.bind_stage_table(
                table_ctx,
                bind_context,
                stage_info,
                files_info,
                alias,
                None,
                None,
            )
            .await
        })
    }
}
