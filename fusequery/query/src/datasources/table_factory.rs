// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::{Arc, Mutex};

use anyhow::Result;
use arrow::datatypes::SchemaRef;
use common_planners::TableOptions;
use indexmap::map::IndexMap;
use lazy_static::lazy_static;

use crate::datasources::local::LocalFactory;
use crate::datasources::ITable;
use crate::sessions::FuseQueryContextRef;

pub struct TableFactory;

pub type TableCreator = fn(
    ctx: FuseQueryContextRef,
    db: String,
    name: String,
    schema: SchemaRef,
    options: TableOptions,
) -> Result<Box<dyn ITable>>;

pub type TableCreatorFactory = Arc<Mutex<IndexMap<&'static str, TableCreator>>>;

lazy_static! {
    static ref FACTORY: TableCreatorFactory = {
        let map: TableCreatorFactory = Arc::new(Mutex::new(IndexMap::new()));
        LocalFactory::register(map.clone()).unwrap();

        map
    };
}

impl TableFactory {
    pub fn create_table(
        engine: &str,
        ctx: FuseQueryContextRef,
        db: String,
        name: String,
        schema: SchemaRef,
        options: TableOptions,
    ) -> Result<Box<dyn ITable>> {
        let map = FACTORY.as_ref().lock()?;
        let creator = map.get(engine).ok_or_else(|| {
            return anyhow::Error::msg(format!("Unsupported Engine: {}", engine));
        })?;
        (creator)(ctx, db, name, schema, options)
    }
}
