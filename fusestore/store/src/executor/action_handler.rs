// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-Lise-Identifier: Apache-2.0.

use std::time::SystemTime;

use anyhow::Context;
use anyhow::Result;
use common_flights::store_do_action::StoreDoAction;
use common_flights::CreateDatabaseAction;
use common_flights::CreateTableAction;

use crate::meta::Catalog;
use crate::meta::DatabaseMeta;
use crate::meta::TableMeta;
use crate::spec::DatabaseSpec;
use crate::spec::TableSpec;

pub struct ActionHandler {
    catalog: Box<dyn Catalog>,
    tbl_spec: TableSpec,
    db_spec: DatabaseSpec,
}

impl ActionHandler {
    pub fn new() -> Self {
        todo!()
        //ActionHandler {
        //}
    }
    pub async fn execute(&self, action: &StoreDoAction) -> Result<()> {
        match action {
            StoreDoAction::CreateDatabase(act) => self.create_db(act).await,
            StoreDoAction::CreateTable(act) => self.create_table(act).await,
            StoreDoAction::ReadPlan(_) => todo!(),
        }
    }

    async fn create_db(&self, act: &CreateDatabaseAction) -> Result<()> {
        let plan = &act.plan;
        let db_meta = DatabaseMeta::from(plan);
        self.db_spec.create_database(&db_meta).await?;
        self.catalog.commit_db_meta(db_meta).await?;
        Ok(())
    }

    async fn create_table(&self, act: &CreateTableAction) -> Result<()> {
        let plan = &act.plan;
        let _db = self
            .catalog
            .get_db(&plan.db)
            .await
            .context("no such database")?;
        let res = self.catalog.get_table(&plan.db, &plan.table).await?;

        if let Some(_meta) = res {
            if plan.if_not_exists {
                anyhow::bail!("table already exist");
            } else {
                // returns silently
                return Ok(());
            }
        }

        let meta = TableMeta {
            table_uuid: uuid::Uuid::new_v4().as_u128(),
            db_name: plan.db.to_string(),
            tbl_name: plan.table.to_string(),
            schema: (*plan.schema).clone(),
            create_ts: SystemTime::now(), // todo utc time
            creator: "".to_string(),
            data_files: vec![],
        };
        let snapshot = self.tbl_spec.create_table(&meta).await?;
        self.catalog.commit_table(&snapshot).await?;
        Ok(())
    }
}
