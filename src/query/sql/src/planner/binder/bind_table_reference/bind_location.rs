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
use databend_common_exception::Result;
use databend_common_meta_app::principal::FileFormatParams;
use databend_common_meta_app::principal::StageFileFormatType;
use databend_common_storage::StageFilesInfo;

use crate::BindContext;
use crate::binder::Binder;
use crate::binder::copy_into_table::resolve_file_location;
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

            let (mut stage_info, path) =
                resolve_file_location(self.ctx.as_ref(), &location).await?;

            if let Some(f) = &options.file_format {
                stage_info.file_format_params = match StageFileFormatType::from_str(f) {
                    Ok(t) => FileFormatParams::default_by_type(t)?,
                    _ => databend_common_base::runtime::block_on(self.ctx.get_file_format(f))?,
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
            let table_ctx = self.ctx.clone();
            self.bind_stage_table(
                table_ctx,
                bind_context,
                stage_info,
                files_info,
                alias,
                None,
                options.case_sensitive.unwrap_or(false),
                None,
            )
            .await
        })
    }
}
