// Copyright 2021 Datafuse Labs.
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

use std::io;
use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_types::GrantObject;
use common_meta_types::UserStageInfo;
use common_tracing::tracing::info;
use futures::TryStreamExt;
use regex::Regex;

use crate::sessions::QueryContext;
use crate::storages::stage::StageSource;

pub async fn validate_grant_object_exists(
    ctx: &Arc<QueryContext>,
    object: &GrantObject,
) -> Result<()> {
    let tenant = ctx.get_tenant();

    match &object {
        GrantObject::Table(catalog_name, database_name, table_name) => {
            let catalog = ctx.get_catalog(catalog_name)?;
            if !catalog
                .exists_table(tenant.as_str(), database_name, table_name)
                .await?
            {
                return Err(common_exception::ErrorCode::UnknownTable(format!(
                    "table {}.{} not exists",
                    database_name, table_name,
                )));
            }
        }
        GrantObject::Database(catalog_name, database_name) => {
            let catalog = ctx.get_catalog(catalog_name)?;
            if !catalog
                .exists_database(tenant.as_str(), database_name)
                .await?
            {
                return Err(common_exception::ErrorCode::UnknownDatabase(format!(
                    "database {} not exists",
                    database_name,
                )));
            }
        }
        GrantObject::Global => (),
    }

    Ok(())
}

pub async fn list_files(
    ctx: Arc<QueryContext>,
    stage: UserStageInfo,
    path: String,
    pattern: String,
) -> Result<Vec<String>> {
    let op = StageSource::get_op(&ctx, &stage).await?;
    let path = &path;
    let pattern = &pattern;
    info!(
        "list stage {:?} with path {path}, pattern {pattern}",
        stage.stage_name
    );

    let mut files = if path.ends_with('/') {
        let mut list = vec![];
        let mut objects = op.object(path).list().await?;
        while let Some(de) = objects.try_next().await? {
            list.push(de.name().to_string());
        }
        list
    } else {
        let o = op.object(path);
        match o.metadata().await {
            Ok(_) => vec![o.name().to_string()],
            Err(e) if e.kind() == io::ErrorKind::NotFound => vec![],
            Err(e) => return Err(e.into()),
        }
    };

    if !pattern.is_empty() {
        let regex = Regex::new(pattern).map_err(|e| {
            ErrorCode::SyntaxException(format!(
                "Pattern format invalid, got:{}, error:{:?}",
                pattern, e
            ))
        })?;

        let matched_files = files
            .into_iter()
            .filter(|file| regex.is_match(file))
            .collect();
        files = matched_files;
    }

    Ok(files)
}
