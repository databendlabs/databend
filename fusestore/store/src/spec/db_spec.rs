// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::path::Path;
use std::sync::Arc;

use crate::io::FS;
use crate::meta::DatabaseMeta;

const DB_META_FILE_NAME: &str = "db_meta.json";

pub struct DatabaseSpec {
    fs: Arc<dyn FS>,
}

impl DatabaseSpec {
    pub async fn create_database(&self, meta: &DatabaseMeta) -> anyhow::Result<()> {
        let db_path = Path::new(&meta.name);
        let content = serde_json::to_string(&meta)?;
        self.fs
            .put_if_absence(&db_path, DB_META_FILE_NAME, content.as_bytes())
            .await
    }
}
