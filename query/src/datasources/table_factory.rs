// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::{Arc, Mutex};

use arrow::datatypes::SchemaRef;
use indexmap::map::IndexMap;
use lazy_static::lazy_static;

use crate::datasources::local::LocalFactory;
use crate::datasources::ITable;
use crate::error::{FuseQueryError, FuseQueryResult};
use crate::planners::TableOptions;
use crate::sessions::FuseQueryContextRef;

pub struct TableFactory;

pub type TableCreator = fn(
    ctx: FuseQueryContextRef,
    db: String,
    name: String,
    schema: SchemaRef,
    options: TableOptions,
) -> FuseQueryResult<Box<dyn ITable>>;

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
    ) -> FuseQueryResult<Box<dyn ITable>> {
        let map = FACTORY.as_ref().lock()?;
        let creator = map.get(engine).ok_or_else(|| {
            FuseQueryError::build_internal_error(format!("Unsupported Engine: {}", engine))
        })?;
        (creator)(ctx, db, name, schema, options)
    }
}
