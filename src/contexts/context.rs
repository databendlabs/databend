// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::Arc;

use super::*;

#[derive(Clone)]
pub struct Context {
    providers: HashMap<String, Arc<dyn IDataSourceProvider>>,
}

impl Context {
    pub fn create() -> Self {
        Context {
            providers: Default::default(),
        }
    }

    pub fn register_datasource(
        &mut self,
        name: &str,
        source: Arc<dyn IDataSourceProvider>,
    ) -> Result<()> {
        self.providers.insert(name.to_string(), source).unwrap();
        Ok(())
    }
}
