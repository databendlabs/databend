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

use common_base::base::tokio;
use common_exception::Result;
use common_meta_types::StageType;
use common_meta_types::UserStageInfo;
use databend_query::sessions::TableContext;
use databend_query::sql::statements::parse_stage_location;
use pretty_assertions::assert_eq;

use crate::tests::create_query_context;

#[tokio::test]
async fn test_parse_stage_location_internal() -> Result<()> {
    let (_guard, ctx) = create_query_context().await?;
    let user_stage_info = UserStageInfo {
        stage_name: "test".to_string(),
        stage_type: StageType::Internal,
        ..Default::default()
    };
    let tenant = ctx.get_tenant();
    // Add an internal stage for testing.
    ctx.get_user_manager()
        .add_stage(&tenant, user_stage_info, false)
        .await?;
    let stage_info = ctx.get_user_manager().get_stage(&tenant, "test").await?;

    // Cases are in the format: `name`, `input location`, `output path`.
    let cases = vec![
        ("root", "@test", "/stage/test/"),
        ("root with trailing /", "@test/", "/stage/test/"),
        (
            "file path",
            "@test/path/to/file",
            "/stage/test/path/to/file",
        ),
        ("dir path", "@test/path/to/dir/", "/stage/test/path/to/dir/"),
    ];

    for (name, input, expected) in cases {
        let table_context: Arc<dyn TableContext> = ctx.clone();
        let (stage, path) = parse_stage_location(&table_context, input).await?;

        assert_eq!(stage, stage_info, "{}", name);
        assert_eq!(path, expected, "{}", name);
    }

    Ok(())
}

#[tokio::test]
async fn test_parse_stage_location_external() -> Result<()> {
    let (_guard, ctx) = create_query_context().await?;
    let user_stage_info = UserStageInfo {
        stage_name: "test".to_string(),
        stage_type: StageType::External,
        ..Default::default()
    };
    let tenant = ctx.get_tenant();
    // Add an external stage for testing.
    ctx.get_user_manager()
        .add_stage(&tenant, user_stage_info, false)
        .await?;
    let stage_info = ctx.get_user_manager().get_stage(&tenant, "test").await?;

    // Cases are in the format: `name`, `input location`, `output path`.
    let cases = vec![
        ("root", "@test", "/"),
        ("root with trailing /", "@test/", "/"),
        ("file path", "@test/path/to/file", "/path/to/file"),
        ("dir path", "@test/path/to/dir/", "/path/to/dir/"),
    ];

    for (name, input, expected) in cases {
        let table_context: Arc<dyn TableContext> = ctx.clone();
        let (stage, path) = parse_stage_location(&table_context, input).await?;

        assert_eq!(stage, stage_info, "{}", name);
        assert_eq!(path, expected, "{}", name);
    }

    Ok(())
}
